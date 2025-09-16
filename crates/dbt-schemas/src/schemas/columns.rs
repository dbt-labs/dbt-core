use std::sync::Arc;

use dbt_common::{adapter::AdapterType, current_function_name};
use minijinja::{
    Value,
    arg_utils::{ArgParser, ArgsIter, check_num_args},
    value::{Enumerator, Object},
};
use serde::{Deserialize, Serialize};

use crate::schemas::{dbt_column::DbtColumn, serde::minijinja_value_to_typed_struct};

/// A struct representing a column type for use with static methods
#[derive(Clone, Debug)]
pub struct StdColumnType(AdapterType);

impl Object for StdColumnType {
    fn call_method(
        self: &Arc<Self>,
        _state: &minijinja::State,
        name: &str,
        args: &[Value],
        _listeners: &[std::rc::Rc<dyn minijinja::listener::RenderingEventListener>],
    ) -> Result<Value, minijinja::Error> {
        match name {
            "create" => self.jinja_create(args),
            "translate_type" => {
                let iter = ArgsIter::new("StdColumnType.translate_type", &["dtype"], args);
                let dtype = iter.next_arg::<&str>()?;
                iter.finish()?;

                Ok(Value::from(self.translate_type(dtype)))
            }
            "numeric_type" => {
                let iter = ArgsIter::new(
                    "StdColumnType.numeric_type",
                    &["dtype", "precision", "scale"],
                    args,
                );
                let dtype = iter.next_arg::<&str>()?;
                let precision: Option<u64> = iter.next_arg::<Option<u64>>()?;
                let scale: Option<u64> = iter.next_arg::<Option<u64>>()?;
                iter.finish()?;

                Ok(Value::from(self.numeric_type(dtype, precision, scale)))
            }
            "string_type" => {
                let iter = ArgsIter::new("StdColumnType.string_type", &["size"], args);
                let size = iter.next_arg::<Option<usize>>()?;
                iter.finish()?;

                Ok(Value::from(self.string_type(size)))
            }
            "from_description" => {
                let iter = ArgsIter::new(
                    "StdColumnType.from_description",
                    &["name", "raw_data_type"],
                    args,
                );
                let name = iter.next_arg::<&str>()?;
                let raw_data_type = iter.next_arg::<&str>()?;
                iter.finish()?;

                self.from_description(name, raw_data_type)
                    .map(Value::from_object)
            }

            // Below are DatabricksColumn-only
            "format_add_column_list" => {
                // TODO: ArgsIter
                let mut args = ArgParser::new(args, None);
                let columns = args.get::<Value>("columns")?;
                let columns = StdColumn::vec_from_jinja_value(AdapterType::Databricks, columns)?;

                Ok(Value::from(self.dbx_format_add_column_list(&columns)?))
            }
            "format_remove_column_list" => {
                // TODO: ArgsIter
                let mut args = ArgParser::new(args, None);
                let columns = args.get::<Value>("columns")?;
                let columns = StdColumn::vec_from_jinja_value(AdapterType::Databricks, columns)?;

                Ok(Value::from(self.dbx_format_remove_column_list(&columns)?))
            }
            "get_name" => {
                let mut args: ArgParser = ArgParser::new(args, None);
                let column = args.get::<Value>("column")?;
                // FIXME: why is this DbtColumn and not StdColumn?
                let column = minijinja_value_to_typed_struct::<DbtColumn>(column).map_err(|e| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::SerdeDeserializeError,
                        e.to_string(),
                    )
                })?;

                Ok(Value::from(self.dbx_get_name(&column)))
            }
            _ => Err(minijinja::Error::new(
                minijinja::ErrorKind::InvalidOperation,
                format!("Unknown method on StdColumnType: '{name}'"),
            )),
        }
    }

    fn call(
        self: &Arc<Self>,
        _state: &minijinja::State,
        args: &[Value],
        _listeners: &[std::rc::Rc<dyn minijinja::listener::RenderingEventListener>],
    ) -> Result<Value, minijinja::Error> {
        self.jinja_create(args)
    }
}

impl StdColumnType {
    pub fn new(adapter_type: AdapterType) -> Self {
        Self(adapter_type)
    }

    fn jinja_create(&self, args: &[Value]) -> Result<Value, minijinja::Error> {
        let iter = ArgsIter::new(
            "StdColumnType.create",
            &[
                "name",
                "label_or_dtype",
                "char_size",
                "numeric_precision",
                "numeric_scale",
            ],
            args,
        );

        let name = iter.next_arg::<&str>()?;
        let dtype = iter.next_arg::<&str>()?;

        let char_size = iter.next_arg::<Option<u32>>().unwrap_or(None);
        let numeric_precision = iter.next_arg::<Option<u64>>().unwrap_or(None);
        let numeric_scale = iter.next_arg::<Option<u64>>().unwrap_or(None);
        iter.finish()?;

        Ok(Value::from_object(self.new_instance(
            name.to_string(),
            dtype.to_string(),
            char_size,
            numeric_precision,
            numeric_scale,
        )))
    }

    /// Create a new column from the given arguments
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/column.py#L28-L29
    pub fn new_instance(
        &self,
        name: String,
        dtype: String,
        char_size: Option<u32>,
        numeric_precision: Option<u64>,
        numeric_scale: Option<u64>,
    ) -> StdColumn {
        StdColumn {
            adapter_attr: AdapterSpecificColumnAttr::default_from_adapter_type(self.0),
            name,
            dtype,
            char_size,
            numeric_precision,
            numeric_scale,
        }
    }

    pub fn quote(&self, s: &str) -> String {
        match self.0 {
            AdapterType::Bigquery | AdapterType::Databricks => format!("`{s}`"),
            _ => format!("\"{s}\""),
        }
    }

    pub fn translate_type(&self, column_type: &str) -> String {
        let translated = match self.0 {
            // https://github.com/dbt-labs/dbt-adapters/blob/6f2aae13e39c5df1c93e5d514678914142d71768/dbt-bigquery/src/dbt/adapters/bigquery/column.py#L16
            AdapterType::Bigquery => match column_type.to_uppercase().as_str() {
                "TEXT" => "STRING",
                "FLOAT" => "FLOAT64",
                "INTEGER" => "INT64",
                _ => column_type,
            },
            AdapterType::Databricks => match column_type.to_uppercase().as_str() {
                "LONG" => "BIGINT",
                _ => column_type,
            },
            // https://github.com/dbt-labs/dbt-adapters/blob/fed0e2e7a2e252175dcc9caccbdd91d354ac6a9d/dbt-adapters/src/dbt/adapters/base/column.py#L24
            _ => match column_type.to_uppercase().as_str() {
                "STRING" => "TEXT",
                _ => column_type,
            },
        };
        translated.to_string()
    }

    pub fn numeric_type(&self, dtype: &str, precision: Option<u64>, scale: Option<u64>) -> String {
        match self.0 {
            // https://github.com/dbt-labs/dbt-adapters/blob/6f2aae13e39c5df1c93e5d514678914142d71768/dbt-bigquery/src/dbt/adapters/bigquery/column.py#L97
            AdapterType::Bigquery => dtype.to_string(),
            _ => match (precision, scale) {
                (Some(p), Some(s)) => format!("{dtype}({p},{s})"),
                _ => dtype.to_string(),
            },
        }
    }

    pub fn string_type(&self, size: Option<usize>) -> String {
        match self.0 {
            // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#string_type
            AdapterType::Bigquery => match size {
                Some(size) => format!("STRING({size})"),
                _ => "STRING".to_string(),
            },
            _ => match size {
                Some(size) => format!("character varying({size})"),
                _ => "character varying".to_string(),
            },
        }
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/column.py#L127-L128
    #[expect(clippy::wrong_self_convention)]
    fn from_description(
        &self,
        name: &str,
        raw_data_type: &str,
    ) -> Result<StdColumn, minijinja::Error> {
        // TODO(serramatutu): why is this Snowflake specific in non-Snowflake specific trait?
        // It seems like it is used by other adapters as well... (tested with BigQuery)
        let mut col = StdColumn::try_from_snowflake_raw_data_type(name, raw_data_type)
            .map_err(|msg| minijinja::Error::new(minijinja::ErrorKind::InvalidArgument, msg))?;
        col.adapter_attr = AdapterSpecificColumnAttr::default_from_adapter_type(self.0);
        Ok(col)
    }

    /// https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/column.py#L66
    fn dbx_format_add_column_list(
        self: &Arc<Self>,
        columns: &[StdColumn],
    ) -> Result<String, minijinja::Error> {
        if self.0 != AdapterType::Databricks {
            unimplemented!("Only available for Databricks")
        };

        Ok(columns
            .iter()
            .map(|c| format!("{} {}", c.quoted(), c.dtype))
            .collect::<Vec<String>>()
            .join(", "))
    }

    /// https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/column.py#L62
    fn dbx_format_remove_column_list(
        self: &Arc<Self>,
        columns: &[StdColumn],
    ) -> Result<String, minijinja::Error> {
        if self.0 != AdapterType::Databricks {
            unimplemented!("Only available for Databricks")
        };

        Ok(columns
            .iter()
            .map(|c| c.quoted().as_str().to_owned())
            .collect::<Vec<String>>()
            .join(", "))
    }

    /// https://github.com/databricks/dbt-databricks/blob/5e20eeaef43e671913f995d8079d4ec2b8a1da6d/dbt/adapters/databricks/column.py#L34
    fn dbx_get_name(self: &Arc<Self>, column: &DbtColumn) -> String {
        if self.0 != AdapterType::Databricks {
            unimplemented!("Only available for Databricks")
        };

        if column.quote.unwrap_or(false) {
            self.quote(&column.name)
        } else {
            column.name.to_string()
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BigqueryColumnMode {
    #[serde(rename = "NULLABLE")]
    Nullable,
    #[serde(rename = "REQUIRED")]
    Required,
    #[serde(rename = "REPEATED")]
    Repeated,
}

impl Default for BigqueryColumnMode {
    fn default() -> Self {
        Self::Nullable
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct BigqueryColumnAttr {
    mode: BigqueryColumnMode,
}

/// Column-attributes that are specific to each adapter.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AdapterSpecificColumnAttr {
    Bigquery(BigqueryColumnAttr),
    Snowflake,
    Redshift,
    Databricks,
    Postgres,
    Salesforce,
}

impl AdapterSpecificColumnAttr {
    pub fn adapter_type(&self) -> AdapterType {
        match self {
            Self::Bigquery(_) => AdapterType::Bigquery,
            Self::Snowflake => AdapterType::Snowflake,
            Self::Redshift => AdapterType::Redshift,
            Self::Databricks => AdapterType::Databricks,
            Self::Postgres => AdapterType::Postgres,
            Self::Salesforce => AdapterType::Salesforce,
        }
    }

    pub fn default_from_adapter_type(at: AdapterType) -> Self {
        match at {
            AdapterType::Bigquery => Self::Bigquery(BigqueryColumnAttr::default()),
            AdapterType::Snowflake => Self::Snowflake,
            AdapterType::Redshift => Self::Redshift,
            AdapterType::Databricks => Self::Databricks,
            AdapterType::Postgres => Self::Postgres,
            AdapterType::Salesforce => Self::Salesforce,
        }
    }
}

/// A struct representing a column
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StdColumn {
    pub adapter_attr: AdapterSpecificColumnAttr,

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

impl StdColumn {
    /// Construct based on a value parsed from dbt Core Jinja
    fn from_dbt_core(adapter_attr: AdapterSpecificColumnAttr, col: DbtCoreBaseColumn) -> Self {
        Self {
            adapter_attr,
            name: col.name,
            dtype: col.dtype,
            char_size: col.char_size,
            numeric_precision: col.numeric_precision,
            numeric_scale: col.numeric_scale,
        }
    }

    /// Get a columns from a jinja value that returns the column in dbt Core format
    pub fn from_jinja_value(
        adapter_type: AdapterType,
        value: Value,
    ) -> Result<Self, minijinja::Error> {
        let core_col =
            minijinja_value_to_typed_struct::<DbtCoreBaseColumn>(value).map_err(|e| {
                minijinja::Error::new(minijinja::ErrorKind::SerdeDeserializeError, e.to_string())
            })?;

        Ok(Self::from_dbt_core(
            AdapterSpecificColumnAttr::default_from_adapter_type(adapter_type),
            core_col,
        ))
    }

    /// Get a vec of columns from a jinja value that returns columns in dbt Core format
    pub fn vec_from_jinja_value(
        adapter_type: AdapterType,
        value: Value,
    ) -> Result<Vec<Self>, minijinja::Error> {
        let result = minijinja_value_to_typed_struct::<Vec<DbtCoreBaseColumn>>(value)
            .map_err(|e| {
                minijinja::Error::new(minijinja::ErrorKind::SerdeDeserializeError, e.to_string())
            })?
            .into_iter()
            // TODO(serramatutu): figure out a way to derive non-standard config here
            .map(|col| {
                Self::from_dbt_core(
                    AdapterSpecificColumnAttr::default_from_adapter_type(adapter_type),
                    col,
                )
            })
            .collect();
        Ok(result)
    }

    pub fn new_bigquery(name: String, dtype: String, mode: BigqueryColumnMode) -> Self {
        Self {
            adapter_attr: AdapterSpecificColumnAttr::Bigquery(BigqueryColumnAttr { mode }),

            name,
            dtype,
            char_size: None,
            numeric_precision: None,
            numeric_scale: None,
        }
    }

    pub fn new_databricks(name: String, dtype: String) -> Self {
        Self {
            adapter_attr: AdapterSpecificColumnAttr::Databricks,

            name,
            dtype,
            char_size: None,
            numeric_precision: None,
            numeric_scale: None,
        }
    }

    pub fn as_static(&self) -> StdColumnType {
        StdColumnType::new(self.adapter_attr.adapter_type())
    }

    /// Parse a Snowflake raw data type into a tuple of (data_type, char_size, numeric_precision, numeric_scale)
    fn try_from_snowflake_raw_data_type(
        name: &str,
        raw_data_type: &str,
    ) -> Result<StdColumn, String> {
        // Parse data type using regex pattern ([^(]+)(\([^)]+\))?
        let re = regex::Regex::new(r"([^(]+)(\([^)]+\))?").expect("A valid regex");
        let captures = re
            .captures(raw_data_type)
            .ok_or_else(|| format!("Could not interpret raw_data_type \"{raw_data_type}\""))?;

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
            format!(
                "Could not interpret data_type \"{raw_data_type}\": could not convert \"{name}\" to an integer"
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
        Ok(Self {
            adapter_attr: AdapterSpecificColumnAttr::Snowflake,

            name: name.to_string(),
            dtype: data_type,
            char_size,
            numeric_precision,
            numeric_scale,
        })
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/column.py#L92-L93
    pub fn string_size(&self) -> Result<u32, String> {
        if !self.is_string() {
            return Err("Called string_size() on non-string field".to_string());
        }

        // FIXME: why self.dtype == "text" instead of is_string()? This is probably a bug...
        if self.dtype == "text" || self.char_size.is_none() {
            let size = match self.adapter_attr {
                AdapterSpecificColumnAttr::Snowflake => 16777216,
                _ => 256,
            };
            Ok(size)
        } else {
            // TODO: this is probably unsafe. But in `dbt-adapters`
            // char_size seems to be unset unless initialized from `from_description` class method
            Ok(self
                .char_size
                .ok_or_else(|| format!("char_size is not set for column: {}", self.name))?)
        }
    }

    fn is_numeric(&self) -> bool {
        match self.adapter_attr {
            AdapterSpecificColumnAttr::Bigquery(_) => {
                matches!(self.dtype.to_lowercase().as_str(), "numeric")
            }
            AdapterSpecificColumnAttr::Snowflake => {
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
            _ => {
                matches!(self.dtype.to_lowercase().as_str(), "numeric" | "decimal")
            }
        }
    }

    fn is_integer(&self) -> bool {
        match self.adapter_attr {
            AdapterSpecificColumnAttr::Bigquery(_) => {
                matches!(self.dtype.to_lowercase().as_str(), "int64")
            }
            AdapterSpecificColumnAttr::Snowflake => false,
            _ => {
                matches!(
                    self.dtype.to_lowercase().as_str(),
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
        }
    }

    fn is_float(&self) -> bool {
        match self.adapter_attr {
            AdapterSpecificColumnAttr::Bigquery(_) => {
                matches!(self.dtype.to_lowercase().as_str(), "float64")
            }
            AdapterSpecificColumnAttr::Snowflake => {
                matches!(
                    self.dtype.to_lowercase().as_str(),
                    "float" | "float4" | "float8" | "double" | "double precision" | "real"
                )
            }
            _ => {
                matches!(
                    self.dtype.to_lowercase().as_str(),
                    "real" | "float4" | "float" | "double precision" | "float8" | "double"
                )
            }
        }
    }

    fn is_number(&self) -> bool {
        self.is_float() || self.is_integer() || self.is_numeric()
    }

    fn is_string(&self) -> bool {
        match self.adapter_attr {
            AdapterSpecificColumnAttr::Bigquery(_) => {
                matches!(self.dtype.to_lowercase().as_str(), "string")
            }
            _ => {
                matches!(
                    self.dtype.to_lowercase().as_str(),
                    "text" | "character varying" | "character" | "varchar"
                )
            }
        }
    }

    fn quoted(&self) -> String {
        self.as_static().quote(&self.name)
    }

    // TODO: impl data_type - need to handle nested types
    // https://github.com/dbt-labs/dbt-adapters/blob/6f2aae13e39c5df1c93e5d514678914142d71768/dbt-bigquery/src/dbt/adapters/bigquery/column.py#L80
    fn data_type(&self) -> String {
        match self.adapter_attr {
            AdapterSpecificColumnAttr::Bigquery(_) => self.dtype.to_lowercase(),
            _ => {
                if self.is_string() {
                    self.as_static().string_type(Some(
                        self.string_size().expect("string should have a size") as usize,
                    ))
                } else if self.is_numeric() {
                    self.as_static().numeric_type(
                        &self.dtype,
                        self.numeric_precision,
                        self.numeric_scale,
                    )
                } else {
                    // TODO for types such as Snowflake TIMESTAMP_LTZ(6), we should return ``format!("{}({})", dtype, precision)``.
                    //  Note that this would not be dbt core compatible behavior, but a more correct one.
                    //  Otherwise we may create/alter a table to a wrong type.
                    //  See also https://github.com/dbt-labs/fs/pull/3585#discussion_r2112390711
                    self.dtype.to_string()
                }
            }
        }
    }

    /// Returns True if this column can be expanded to the size of the other column
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/column.py#L102-L103
    ///
    /// # Panics
    ///
    /// This function will panic if the column is not a string.
    pub fn can_expand_to(&self, other: &StdColumn) -> Result<bool, minijinja::Error> {
        Ok(self.is_string()
            && other.is_string()
            && self
                .string_size()
                .map_err(|msg| minijinja::Error::new(minijinja::ErrorKind::MissingArgument, msg))?
                < other.string_size().map_err(|msg| {
                    minijinja::Error::new(minijinja::ErrorKind::MissingArgument, msg)
                })?)
    }
}

impl Object for StdColumn {
    fn call_method(
        self: &Arc<Self>,
        _state: &minijinja::State,
        name: &str,
        args: &[Value],
        _listeners: &[std::rc::Rc<dyn minijinja::listener::RenderingEventListener>],
    ) -> Result<Value, minijinja::Error> {
        match name {
            "is_string" => Ok(Value::from(self.is_string())),
            "string_size" => Ok(Value::from(self.string_size().map_err(|msg| {
                minijinja::Error::new(minijinja::ErrorKind::InvalidArgument, msg)
            })?)),
            "is_number" => Ok(Value::from(self.is_number())),
            "is_float" => Ok(Value::from(self.is_float())),
            "is_integer" => Ok(Value::from(self.is_integer())),
            "is_numeric" => Ok(Value::from(self.is_numeric())),
            "can_expand_to" => {
                // TODO(serramatutu): use ArgsIter
                let mut parser = ArgParser::new(args, None);
                check_num_args(current_function_name!(), &parser, 1, 1)?;
                let other_raw = parser.get::<Value>("other_column")?;
                let other =
                    StdColumn::from_jinja_value(self.adapter_attr.adapter_type(), other_raw)?;
                Ok(Value::from(self.can_expand_to(&other)?))
            }
            _ => Err(minijinja::Error::new(
                minijinja::ErrorKind::InvalidOperation,
                format!("Unknown method on StdColumn: '{name}'"),
            )),
        }
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str() {
            Some("name") | Some("column") => Some(Value::from(&self.name)),
            Some("quoted") => Some(Value::from(self.quoted())),
            Some("data_type") => Some(Value::from(self.data_type())),
            Some("dtype") => Some(Value::from(&self.dtype)),
            Some("char_size") => Some(Value::from(self.char_size)),
            Some("numeric_precision") => Some(Value::from(self.numeric_precision)),
            Some("numeric_scale") => Some(Value::from(self.numeric_scale)),
            Some("mode") => {
                if let AdapterSpecificColumnAttr::Bigquery(bq) = &self.adapter_attr {
                    Some(Value::from_serialize(&bq.mode))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        let mut keys = vec![
            "name",
            "dtype",
            "char_size",
            "column",
            "quoted",
            "numeric_precision",
            "numeric_scale",
        ];

        if let AdapterSpecificColumnAttr::Bigquery(_bq) = &self.adapter_attr {
            keys.push("mode");
        }

        Enumerator::Iter(Box::new(keys.into_iter().map(Value::from)))
    }
}

#[expect(clippy::from_over_into)]
impl Into<Value> for StdColumn {
    fn into(self) -> Value {
        Value::from_object(self)
    }
}

/// The BaseColumn as implemented by dbt Core.
///
/// This is used to deserialize columns from Jinja that produces them, for example
/// the public API macros for `get_columns_in_relation()`
#[derive(Deserialize, Debug)]
struct DbtCoreBaseColumn {
    name: String,
    dtype: String,
    char_size: Option<u32>,
    numeric_precision: Option<u64>,
    numeric_scale: Option<u64>,
}
