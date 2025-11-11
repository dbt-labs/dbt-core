use dbt_common::adapter::AdapterType;
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
        if b {
            "true".to_string()
        } else {
            "false".to_string()
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

    pub fn none_value(&self) -> String {
        "NULL".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
