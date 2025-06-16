use super::AdapterType;

pub fn convert_integer_type(adapter_type: AdapterType) -> String {
    let result = match adapter_type {
        // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L358
        AdapterType::Bigquery => "int64",
        // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-spark/src/dbt/adapters/spark/impl.py#L145-L146
        AdapterType::Databricks => "bigint",
        // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py
        _ => "integer",
    };
    result.to_string()
}

/// [convert_floating_type] and [convert_decimal_type]
/// are splitted from [`convert_number_type`](https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L76-L77)
pub fn convert_floating_type(adapter_type: AdapterType) -> String {
    let result = match adapter_type {
        // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L351-L352
        AdapterType::Bigquery => "int64",
        // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-spark/src/dbt/adapters/spark/impl.py#L138-L139
        AdapterType::Databricks => "bigint",
        _ => "integer",
    };
    result.to_string()
}

/// [convert_floating_type] and [convert_decimal_type]
/// are splitted from [`convert_number_type`](https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L76-L77)
pub fn convert_decimal_type(adapter_type: AdapterType) -> String {
    let result = match adapter_type {
        // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L351-L352
        AdapterType::Bigquery => "float64",
        // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-spark/src/dbt/adapters/spark/impl.py#L138-L139
        AdapterType::Databricks => "double",
        // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L76-L77
        _ => "float8",
    };
    result.to_string()
}

pub fn convert_boolean_type(adapter_type: AdapterType) -> String {
    let result = match adapter_type {
        // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L362-L363
        AdapterType::Bigquery => "bool",
        // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L88-L89
        _ => "boolean",
    };
    result.to_string()
}

pub fn convert_datetime_type(adapter_type: AdapterType) -> String {
    let result = match adapter_type {
        // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L366-L367
        AdapterType::Bigquery => "datetime",
        // https://github.com/dbt-labs/dbt-adapters/blob/2a94cc75dba1f98fa5caff1f396f5af7ee444598/dbt-spark/src/dbt/adapters/spark/impl.py#L158
        AdapterType::Databricks => "timestamp",
        // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L92-L93
        _ => "timestamp without time zone",
    };
    result.to_string()
}

pub fn convert_date_type(_adapter_type: AdapterType) -> String {
    // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L96
    // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L370-L371
    "date".to_string()
}

pub fn convert_time_type(_adapter_type: AdapterType) -> String {
    // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L366-L367
    // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L374-L375
    "time".to_string()
}

pub fn convert_text_type(adapter_type: AdapterType) -> String {
    let result = match adapter_type {
        // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L347
        // https://github.com/dbt-labs/dbt-adapters/blob/2a94cc75dba1f98fa5caff1f396f5af7ee444598/dbt-spark/src/dbt/adapters/spark/impl.py#L134
        AdapterType::Bigquery | AdapterType::Databricks => "string",
        // https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/sql/impl.py#L72-L73
        _ => "text",
    };
    result.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_integer_type() {
        assert_eq!(convert_integer_type(AdapterType::Bigquery), "int64");
        assert_eq!(convert_integer_type(AdapterType::Databricks), "bigint");
        assert_eq!(convert_integer_type(AdapterType::Postgres), "integer");
        assert_eq!(convert_integer_type(AdapterType::Snowflake), "integer");
        assert_eq!(convert_integer_type(AdapterType::Redshift), "integer");
    }

    #[test]
    fn test_convert_floating_type() {
        assert_eq!(convert_floating_type(AdapterType::Bigquery), "int64");
        assert_eq!(convert_floating_type(AdapterType::Databricks), "bigint");
        assert_eq!(convert_floating_type(AdapterType::Postgres), "integer");
        assert_eq!(convert_floating_type(AdapterType::Snowflake), "integer");
        assert_eq!(convert_floating_type(AdapterType::Redshift), "integer");
    }

    #[test]
    fn test_convert_decimal_type() {
        assert_eq!(convert_decimal_type(AdapterType::Bigquery), "float64");
        assert_eq!(convert_decimal_type(AdapterType::Databricks), "double");
        assert_eq!(convert_decimal_type(AdapterType::Postgres), "float8");
        assert_eq!(convert_decimal_type(AdapterType::Snowflake), "float8");
        assert_eq!(convert_decimal_type(AdapterType::Redshift), "float8");
    }

    #[test]
    fn test_convert_boolean_type() {
        assert_eq!(convert_boolean_type(AdapterType::Bigquery), "bool");
        assert_eq!(convert_boolean_type(AdapterType::Databricks), "boolean");
        assert_eq!(convert_boolean_type(AdapterType::Postgres), "boolean");
        assert_eq!(convert_boolean_type(AdapterType::Snowflake), "boolean");
        assert_eq!(convert_boolean_type(AdapterType::Redshift), "boolean");
    }

    #[test]
    fn test_convert_datetime_type() {
        assert_eq!(convert_datetime_type(AdapterType::Bigquery), "datetime");
        assert_eq!(convert_datetime_type(AdapterType::Databricks), "timestamp");
        assert_eq!(
            convert_datetime_type(AdapterType::Postgres),
            "timestamp without time zone"
        );
        assert_eq!(
            convert_datetime_type(AdapterType::Snowflake),
            "timestamp without time zone"
        );
        assert_eq!(
            convert_datetime_type(AdapterType::Redshift),
            "timestamp without time zone"
        );
    }

    #[test]
    fn test_convert_date_type() {
        // Test all adapters return "date"
        for adapter_type in [
            AdapterType::Bigquery,
            AdapterType::Databricks,
            AdapterType::Postgres,
            AdapterType::Snowflake,
            AdapterType::Redshift,
        ] {
            assert_eq!(convert_date_type(adapter_type), "date");
        }
    }

    #[test]
    fn test_convert_time_type() {
        // Test all adapters return "time"
        for adapter_type in [
            AdapterType::Bigquery,
            AdapterType::Databricks,
            AdapterType::Postgres,
            AdapterType::Snowflake,
            AdapterType::Redshift,
        ] {
            assert_eq!(convert_time_type(adapter_type), "time");
        }
    }

    #[test]
    fn test_convert_text_type() {
        assert_eq!(convert_text_type(AdapterType::Bigquery), "string");
        assert_eq!(convert_text_type(AdapterType::Databricks), "string");
        assert_eq!(convert_text_type(AdapterType::Postgres), "text");
        assert_eq!(convert_text_type(AdapterType::Snowflake), "text");
        assert_eq!(convert_text_type(AdapterType::Redshift), "text");
    }
}
