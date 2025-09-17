use std::borrow::Cow;

use crate::errors::{AdapterError, AdapterErrorKind};
use arrow_schema::DataType;
use dbt_common::adapter::AdapterType;

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
