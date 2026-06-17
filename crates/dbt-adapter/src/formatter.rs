use chrono::{DateTime, SecondsFormat, Utc};
use dbt_adapter_core::AdapterType;
use dbt_common::{AdapterError, AdapterErrorKind, AdapterResult};
use minijinja::Value;
use minijinja::value::ValueKind;
use minijinja_contrib::modules::py_datetime::date::PyDate;
use minijinja_contrib::modules::py_datetime::datetime::PyDateTime;

/// Formatter for SQL Literals.
///
/// Differences in SQL dialects are handled by matching on the [AdapterType].
pub struct SqlLiteralFormatter {
    adapter_type: AdapterType,
}

impl SqlLiteralFormatter {
    pub fn new(adapter_type: AdapterType) -> Self {
        Self { adapter_type }
    }

    pub fn format_bool(&self, b: bool) -> String {
        match self.adapter_type {
            AdapterType::Fabric => {
                if b {
                    "1".to_string()
                } else {
                    "0".to_string()
                }
            }
            _ => {
                if b {
                    "true".to_string()
                } else {
                    "false".to_string()
                }
            }
        }
    }

    pub fn format_str(&self, l: &str) -> String {
        match self.adapter_type {
            AdapterType::Bigquery | AdapterType::Databricks => {
                // BigQuery and Databricks uses \ for string escapes
                // https://docs.databricks.com/aws/en/sql/language-manual/data-types/string-type
                let escaped_str = l.replace("'", "\\'");
                format!("'{escaped_str}'")
            }
            AdapterType::Snowflake => {
                let escaped_str = l.replace('\\', "\\\\").replace('\'', "''");
                format!("'{escaped_str}'")
            }
            _ => {
                // XXX: this of course not enough for all strings in any SQL dialect
                // but it's a start
                let escaped_str = l.replace("'", "''");
                format!("'{escaped_str}'")
            }
        }
    }

    /// ## Panics
    /// If the value is not a bytes array
    pub fn format_bytes(&self, bytes_value: &Value) -> String {
        assert!(bytes_value.kind() == ValueKind::Bytes);
        // uses what is defined by impl fmt::Display for Value
        format!("'{bytes_value}'")
    }

    pub fn format_date(&self, l: PyDate) -> String {
        format!("'{}'", l.date.format("%Y-%m-%d"))
    }

    pub fn format_datetime(&self, l: PyDateTime) -> String {
        format!("'{}'", l.isoformat())
    }

    /// Format a UTC timestamp as a SQL literal for this adapter.
    ///
    /// RFC 3339 allows any number of fractional-second digits (`"." 1*DIGIT`); chrono
    /// emits nanoseconds by default because `DateTime<Utc>` is two `u32`s internally
    /// (seconds + nanoseconds). BigQuery's TIMESTAMP parser is stricter than the RFC
    /// and caps at microseconds, so we truncate to 6 digits to avoid a runtime parse error.
    pub fn format_timestamp(&self, ts: DateTime<Utc>) -> String {
        match self.adapter_type {
            AdapterType::Bigquery => ts.to_rfc3339_opts(SecondsFormat::Micros, true),
            _ => ts.to_rfc3339(),
        }
    }

    pub fn none_value(&self) -> String {
        "NULL".to_string()
    }
}

pub fn format_sql_with_bindings(
    adapter_type: AdapterType,
    sql: &str,
    bindings: &Value,
) -> AdapterResult<String> {
    let formatter = SqlLiteralFormatter::new(adapter_type);
    let mut result = String::with_capacity(sql.len());
    // this placeholder char is seen from `get_binding_char` macro
    let binding_char = if adapter_type == AdapterType::Fabric {
        "?"
    } else {
        "%s"
    };
    let mut parts = sql.split(binding_char);
    let mut binding_iter = bindings.as_object().unwrap().try_iter().unwrap();

    // Add the first part (before any %s)
    if let Some(first) = parts.next() {
        result.push_str(first);
    }

    // For each remaining part, insert a binding value before it
    for part in parts {
        match binding_iter.next() {
            Some(value) => {
                // Convert minijinja::Value to a SQL-safe string
                match value.kind() {
                    ValueKind::String => {
                        result.push_str(&formatter.format_str(value.as_str().unwrap()))
                    }
                    ValueKind::Bytes => result.push_str(&formatter.format_bytes(&value)),
                    ValueKind::None => result.push_str(&formatter.none_value()),
                    ValueKind::Bool => result.push_str(&formatter.format_bool(value.is_true())),
                    _ => {
                        // TODO: handle the SQL escaping of more data types
                        if let Some(date) = value.downcast_object::<PyDate>() {
                            result.push_str(&formatter.format_date(date.as_ref().clone()));
                        } else if let Some(datetime) = value.downcast_object::<PyDateTime>() {
                            result.push_str(&formatter.format_datetime(datetime.as_ref().clone()));
                        } else {
                            result.push_str(&value.to_string())
                        }
                    }
                }
            }
            None => {
                return Err(AdapterError::new(
                    AdapterErrorKind::Configuration,
                    "Not enough bindings provided for SQL template".to_string(),
                ));
            }
        }
        result.push_str(part);
    }

    // Check if we used all bindings
    if binding_iter.next().is_some() {
        return Err(AdapterError::new(
            AdapterErrorKind::Configuration,
            "Too many bindings provided for SQL template".to_string(),
        ));
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_timestamp_bigquery_truncates_to_microseconds() {
        // BigQuery rejects nanosecond-precision RFC 3339 strings at parse time.
        // Verify we emit exactly 6 fractional digits regardless of input precision.
        let ts = DateTime::from_timestamp(1_700_000_000, 999).unwrap(); // 999 ns
        let result = SqlLiteralFormatter::new(AdapterType::Bigquery).format_timestamp(ts);
        let frac = result.split('.').nth(1).unwrap();
        assert_eq!(
            &frac[..6],
            "000000",
            "expected 6 fractional digits, got: {result}"
        );
        assert!(
            !frac.chars().nth(6).is_some_and(|c| c.is_ascii_digit()),
            "must not emit sub-microsecond digits: {result}"
        );
    }

    #[test]
    fn format_timestamp_non_bigquery_preserves_nanoseconds() {
        // Non-BigQuery adapters must round-trip sub-microsecond precision;
        // truncating here would silently corrupt time windows on those platforms.
        let ts = DateTime::from_timestamp(1_700_000_000, 999).unwrap(); // 999 ns
        let result = SqlLiteralFormatter::new(AdapterType::Snowflake).format_timestamp(ts);
        assert!(
            result.contains("000000999"),
            "expected nanoseconds in output, got: {result}"
        );
    }

    #[test]
    fn format_timestamp_bigquery_uses_utc_z_suffix() {
        // BigQuery TIMESTAMP literals must carry an explicit UTC indicator.
        // to_rfc3339_opts with use_z=true produces "Z"; confirm we don't emit "+00:00".
        let ts = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let result = SqlLiteralFormatter::new(AdapterType::Bigquery).format_timestamp(ts);
        assert!(
            result.ends_with('Z'),
            "expected Z suffix for UTC, got: {result}"
        );
    }

    #[test]
    fn test_bigquery_format_str() {
        let formatter = SqlLiteralFormatter::new(AdapterType::Bigquery);
        assert_eq!(formatter.format_str("hello"), "'hello'");
        assert_eq!(formatter.format_str("it's"), "'it\\'s'");
        assert_eq!(formatter.format_str("it's a test's"), "'it\\'s a test\\'s'");
        assert_eq!(formatter.format_str(""), "''");
        assert_eq!(formatter.format_str("\\"), "'\\'");
        assert_eq!(formatter.format_str("\\'"), "'\\\\''");
    }

    #[test]
    fn test_databricks_format_str() {
        let formatter = SqlLiteralFormatter::new(AdapterType::Databricks);

        assert_eq!(formatter.format_str("hello"), "'hello'");
        assert_eq!(formatter.format_str("it's"), "'it\\'s'");
        assert_eq!(formatter.format_str("it's a test's"), "'it\\'s a test\\'s'");
        assert_eq!(formatter.format_str(""), "''");
        assert_eq!(formatter.format_str("\\"), "'\\'");
        assert_eq!(formatter.format_str("\\'"), "'\\\\''");
    }

    #[test]
    fn test_snowflake_format_str() {
        let f = SqlLiteralFormatter::new(AdapterType::Snowflake);

        assert_eq!(f.format_str(""), "''");
        assert_eq!(f.format_str("hello"), "'hello'");
        assert_eq!(f.format_str("Mom\\Baby"), "'Mom\\\\Baby'");
    }
}
