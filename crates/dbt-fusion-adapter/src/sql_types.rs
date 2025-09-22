use std::borrow::Cow;

use crate::AdapterResult;
use crate::base_adapter::backend_of;
use crate::errors::{AdapterError, AdapterErrorKind};
use arrow_schema::DataType;
use dbt_common::adapter::AdapterType;
use dbt_xdbc::sql::types::SqlType;

pub trait TypeFormatter: Send + Sync {
    /// Picks a SQL type for a given Arrow DataType and renders it as SQL.
    ///
    /// The implementation is dialect-specific.
    fn format_arrow_type_as_sql(&self, data_type: &DataType, out: &mut String)
    -> AdapterResult<()>;

    /// Renders a given SqlType as SQL.
    ///
    /// The implementation is dialect-specific.
    fn format_sql_type(&self, sql_type: SqlType, out: &mut String) -> AdapterResult<()>;
}

pub struct NaiveTypeFormatterImpl(AdapterType, dbt_xdbc::Backend);

impl NaiveTypeFormatterImpl {
    pub fn new(adapter_type: AdapterType) -> Self {
        let backend = backend_of(adapter_type);
        Self(adapter_type, backend)
    }
}

impl TypeFormatter for NaiveTypeFormatterImpl {
    fn format_arrow_type_as_sql(
        &self,
        data_type: &DataType,
        out: &mut String,
    ) -> AdapterResult<()> {
        let adapter_type = self.0;
        match adapter_type {
            AdapterType::Postgres | AdapterType::Salesforce => {
                postgres::try_format_type(data_type, true, out)
            }
            _ => {
                let hint: SqlTypeHint = data_type.try_into()?;
                // TODO: handle has_decimal_places correctly
                let has_decimal_places = false;
                let res = sql_type_hint_to_str(hint, has_decimal_places, adapter_type);
                out.push_str(res.as_ref());
                Ok(())
            }
        }
    }

    fn format_sql_type(&self, sql_type: SqlType, out: &mut String) -> AdapterResult<()> {
        let backend = self.1;
        sql_type.write(backend, out).map_err(|e| {
            AdapterError::new(
                AdapterErrorKind::NotSupported,
                format!("Failed to convert SQL type {sql_type:?}. Error: {e}"),
            )
        })
    }
}

pub enum SqlTypeHint {
    Integer,
    Floating,
    Decimal,
    Boolean,
    Datetime,
    Date,
    Time,
    Text,
}

impl TryFrom<&DataType> for SqlTypeHint {
    type Error = AdapterError;

    fn try_from(data_type: &DataType) -> Result<SqlTypeHint, Self::Error> {
        use SqlTypeHint::*;
        let hint = if data_type.is_null() {
            Text
        } else if data_type.is_integer() {
            Integer
        } else if data_type.is_floating() {
            Floating
        } else if data_type.is_numeric() {
            Decimal
        } else if *data_type == DataType::Boolean {
            Boolean
        } else if matches!(data_type, DataType::Timestamp(_, _)) {
            Datetime
        } else if matches!(data_type, DataType::Date32 | DataType::Date64) {
            Date
        } else if matches!(data_type, DataType::Duration(_) | DataType::Interval(_)) {
            Time
        } else if matches!(data_type, DataType::Utf8) {
            Text
        } else {
            return Err(AdapterError::new(
                AdapterErrorKind::NotSupported,
                format!("Unsupported data type {data_type:?}"),
            ));
        };
        Ok(hint)
    }
}

/// A function that replaces all `convert_{type}_type` functions [1].
///
/// The functions are:
/// - `convert_integer_type`
/// - `convert_number_type` (floating and decimal types)
/// - `convert_boolean_type`
/// - `convert_datetime_type`
/// - `convert_date_type`
/// - `convert_time_type`
/// - `convert_text_type`
///
/// Databricks uses the conversion rules from Spark [3].
///
/// [1] https://github.com/dbt-labs/dbt-adapters/blob/b0223a88d67012bcc4c6cce5449c4fe10c6ed198/dbt-adapters/src/dbt/adapters/sql/impl.py
/// [2] https://github.com/dbt-labs/dbt-adapters/blob/b0223a88d67012bcc4c6cce5449c4fe10c6ed198/dbt-bigquery/src/dbt/adapters/bigquery/impl.py
/// [3] https://github.com/dbt-labs/dbt-adapters/blob/b0223a88d67012bcc4c6cce5449c4fe10c6ed198/dbt-spark/src/dbt/adapters/spark/impl.py
pub fn sql_type_hint_to_str<'a>(
    hint: SqlTypeHint,
    _has_decimal_places: bool,
    adapter_type: AdapterType,
) -> Cow<'a, str> {
    use SqlTypeHint::*;
    use dbt_common::adapter::AdapterType::*;
    let str = match (adapter_type, hint) {
        // ## convert_integer_type()
        (Bigquery, Integer) => "int64",
        (Databricks, Integer) => "bigint",
        (_, Integer) => "integer",

        // ## convert_number_type()
        (Bigquery, Floating) => "int64", // TODO: fix to "float64" if has_decimal_places is true
        (Bigquery, Decimal) => "float64", // TODO: fix to "int64" if has_decimal_places is false
        (Databricks, Floating) => "bigint", // TODO: fix to "double" if has_decimal_places is true
        (Databricks, Decimal) => "double", // TODO: fix to "bigint" if has_decimal_places is false
        (_, Floating) => "integer",      // TODO: fix to "float8" if has_decimal_places is true
        (_, Decimal) => "float8",        // TODO: fix to "integer" if has_decimal_places is false

        // ## convert_boolean_type()
        (Bigquery, Boolean) => "bool",
        (_, Boolean) => "boolean",

        // ## convert_datetime_type()
        (Bigquery, Datetime) => "datetime",
        (Databricks, Datetime) => "timestamp",
        (_, Datetime) => "timestamp without time zone",

        // ## convert_date_type()
        (_, Date) => "date",

        // ## convert_time_type()
        (_, Time) => "time",

        // ## convert_text_type()
        (Bigquery | Databricks, Text) => "string",
        (_, Text) => "text",
    };
    Cow::Borrowed(str)
}

pub mod postgres {
    use arrow_schema::{DataType, TimeUnit};

    use crate::AdapterResult;
    use crate::errors::{AdapterError, AdapterErrorKind};

    pub fn try_format_type(
        datatype: &DataType,
        nullable: bool,
        out: &mut String,
    ) -> AdapterResult<()> {
        use std::fmt::Write as _;
        match datatype {
            DataType::Null => out.push_str("null"),
            DataType::Boolean => out.push_str("boolean"),
            DataType::Int8 => out.push_str("tinyint"),
            DataType::Int16 => out.push_str("smallint"),
            DataType::Int32 => out.push_str("integer"),
            DataType::Int64 => out.push_str("bigint"),
            DataType::UInt8 => out.push_str("tinyint"),
            DataType::UInt16 => out.push_str("smallint"),
            DataType::UInt32 => out.push_str("integer"),
            DataType::UInt64 => out.push_str("bigint"),
            DataType::Float32 => out.push_str("real"),
            DataType::Float64 => out.push_str("double"),
            DataType::Timestamp(TimeUnit::Second, _) => out.push_str("timestamp without time zone"),
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                out.push_str("timestamp without time zone")
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                out.push_str("timestamp without time zone")
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                out.push_str("timestamp without time zone")
            }
            DataType::Date32 => out.push_str("date"),
            DataType::Time32(TimeUnit::Second) => out.push_str("time without time zone"),
            DataType::Time32(TimeUnit::Millisecond) => out.push_str("time without time zone"),
            DataType::Time64(TimeUnit::Microsecond) => out.push_str("time without time zone"),
            DataType::Time64(TimeUnit::Nanosecond) => out.push_str("time without time zone"),
            DataType::Interval(_) => out.push_str("interval"),
            DataType::Binary => out.push_str("binary"),
            DataType::Utf8 | DataType::Utf8View => out.push_str("text"),
            DataType::List(_) => out.push_str("array"),
            DataType::Dictionary(key, value)
                if key.as_ref() == &DataType::UInt16 && value.as_ref() == &DataType::Utf8 =>
            {
                out.push_str("text")
            }
            DataType::Decimal128(precision, scale) => {
                write!(out, "decimal({precision}, {scale})").unwrap()
            }
            _ => {
                return Err(AdapterError::new(
                    AdapterErrorKind::UnsupportedType,
                    format!("{datatype} is not convertible to postgres type"),
                ));
            }
        };
        if !nullable {
            out.push_str(" not null");
        }
        Ok(())
    }
}

pub mod redshift {
    use arrow_schema::DataType;

    use crate::AdapterResult;
    use crate::errors::{AdapterError, AdapterErrorKind};

    const VARCHAR_DEFAULT: usize = 256;
    const VARBYTE_DEFAULT: usize = 65535;

    /// The size constraint for variable-size types (e.g. VARCHAR, VARBINARY).
    pub fn var_size(data_type: &DataType) -> Option<usize> {
        match data_type {
            // Strings: Redshift wants a length; persist it in char_size
            // TODO(jason): We need to report the correct size and not just a default
            DataType::Utf8 | DataType::Utf8View => Some(VARCHAR_DEFAULT),
            // Bytes
            // TODO(jason): We need to report the correct size and not just a default
            DataType::Binary => Some(VARBYTE_DEFAULT),

            // Dictionary<UInt16, Utf8> - rendered as varchar which needs a length
            DataType::Dictionary(key, value)
                if key.as_ref() == &DataType::UInt16 && value.as_ref() == &DataType::Utf8 =>
            {
                Some(VARCHAR_DEFAULT)
            }

            _ => None,
        }
    }

    pub fn numeric_precision_scale(
        data_type: &DataType,
    ) -> AdapterResult<Option<(u8, Option<i8>)>> {
        let precision_scale = match data_type {
            // For Decimal types, extract precision and scale; cap at 38
            DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
                if *precision > 38 {
                    return Err(AdapterError::new(
                        AdapterErrorKind::NotSupported,
                        format!("Decimal precision '{}' exceed 38 place limit", *precision),
                    ));
                }
                Some((*precision, Some(*scale)))
            }

            // For integer types (i.e. non-scaled numbers)
            DataType::Int16 => Some((5, None)),
            DataType::Int32 => Some((10, None)),
            DataType::Int64 => Some((19, None)),
            DataType::UInt16 => Some((5, None)),
            DataType::UInt32 => Some((10, None)),
            DataType::UInt64 => Some((20, None)),

            // For floating point types (i.e. arbitrarily scaled numbers)
            DataType::Float32 => Some((24, None)),
            DataType::Float64 => Some((53, None)),

            DataType::Time64(_) | DataType::Time32(_) => {
                // Redshift stores microseconds (6 fractional digits)
                Some((6, None))
            }
            // Timestamps (with or without tz) – clamp to microseconds
            // TODO: handle more complex timestamp/date/time types not in sdk front end
            DataType::Timestamp(_, _) => Some((6, None)),

            // Other types don't have specific precision/scale
            _ => None,
        };

        Ok(precision_scale)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use SqlTypeHint::*;
    use dbt_common::adapter::AdapterType::*;

    #[test]
    fn test_convert_integer_type() {
        let convert_integer_type =
            |adapter_type| sql_type_hint_to_str(Integer, false, adapter_type);
        assert_eq!(convert_integer_type(Bigquery), "int64");
        assert_eq!(convert_integer_type(Databricks), "bigint");
        assert_eq!(convert_integer_type(Postgres), "integer");
        assert_eq!(convert_integer_type(Snowflake), "integer");
        assert_eq!(convert_integer_type(Redshift), "integer");
    }

    #[test]
    fn test_convert_number_type() {
        let convert_floating_type =
            |adapter_type| sql_type_hint_to_str(Floating, false, adapter_type);
        assert_eq!(convert_floating_type(Bigquery), "int64");
        assert_eq!(convert_floating_type(Databricks), "bigint");
        assert_eq!(convert_floating_type(Postgres), "integer");
        assert_eq!(convert_floating_type(Snowflake), "integer");
        assert_eq!(convert_floating_type(Redshift), "integer");
        let convert_decimal_type =
            |adapter_type| sql_type_hint_to_str(Decimal, false, adapter_type);
        assert_eq!(convert_decimal_type(Bigquery), "float64");
        assert_eq!(convert_decimal_type(Databricks), "double");
        assert_eq!(convert_decimal_type(Postgres), "float8");
        assert_eq!(convert_decimal_type(Snowflake), "float8");
        assert_eq!(convert_decimal_type(Redshift), "float8");
    }

    #[test]
    fn test_convert_boolean_type() {
        let convert_boolean_type =
            |adapter_type| sql_type_hint_to_str(Boolean, false, adapter_type);
        assert_eq!(convert_boolean_type(Bigquery), "bool");
        assert_eq!(convert_boolean_type(Databricks), "boolean");
        assert_eq!(convert_boolean_type(Postgres), "boolean");
        assert_eq!(convert_boolean_type(Snowflake), "boolean");
        assert_eq!(convert_boolean_type(Redshift), "boolean");
    }

    #[test]
    fn test_convert_datetime_type() {
        let convert_datetime_type =
            |adapter_type| sql_type_hint_to_str(Datetime, false, adapter_type);
        assert_eq!(convert_datetime_type(Bigquery), "datetime");
        assert_eq!(convert_datetime_type(Databricks), "timestamp");
        assert_eq!(
            convert_datetime_type(Postgres),
            "timestamp without time zone"
        );
        assert_eq!(
            convert_datetime_type(Snowflake),
            "timestamp without time zone"
        );
        assert_eq!(
            convert_datetime_type(Redshift),
            "timestamp without time zone"
        );
    }
    const ALL_ADAPTERS: [AdapterType; 5] = [Bigquery, Databricks, Postgres, Snowflake, Redshift];

    #[test]
    fn test_convert_date_type() {
        let convert_date_type = |adapter_type| sql_type_hint_to_str(Date, false, adapter_type);
        // Test all adapters return "date"
        for adapter_type in ALL_ADAPTERS {
            assert_eq!(convert_date_type(adapter_type), "date");
        }
    }

    #[test]
    fn test_convert_time_type() {
        let convert_time_type = |adapter_type| sql_type_hint_to_str(Time, false, adapter_type);
        // Test all adapters return "time"
        for adapter_type in ALL_ADAPTERS {
            assert_eq!(convert_time_type(adapter_type), "time");
        }
    }

    #[test]
    fn test_convert_text_type() {
        let convert_text_type = |adapter_type| sql_type_hint_to_str(Text, false, adapter_type);
        assert_eq!(convert_text_type(Bigquery), "string");
        assert_eq!(convert_text_type(Databricks), "string");
        assert_eq!(convert_text_type(Postgres), "text");
        assert_eq!(convert_text_type(Snowflake), "text");
        assert_eq!(convert_text_type(Redshift), "text");
    }
}
