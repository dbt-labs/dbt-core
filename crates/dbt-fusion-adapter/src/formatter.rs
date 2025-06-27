use minijinja_contrib::modules::py_datetime::date::PyDate;
use minijinja_contrib::modules::py_datetime::datetime::PyDateTime;

use crate::bigquery::formatter::BigquerySqlLiteralFormatter;
use crate::databricks::formatter::DatabricksSqlLiteralFormatter;
use crate::postgres::formatter::PostgreSqlLiteralFormatter;
use crate::redshift::formatter::RedshiftSqlLiteralFormatter;
use crate::snowflake::formatter::SnowflakeSqlLiteralFormatter;
use crate::AdapterType;

/// Formatter for SQL Literals
/// This trait contains default implementations based on the SQL standard
pub trait SqlLiteralFormatter {
    fn format_str(&self, l: &str) -> String {
        let escaped_str = l.replace("'", "''");
        format!("'{escaped_str}'")
    }

    fn format_date(&self, l: PyDate) -> String {
        format!("'{}'", l.date.format("%Y-%m-%d"))
    }

    fn format_datetime(&self, l: PyDateTime) -> String {
        format!("'{}'", l.isoformat())
    }

    fn none_value(&self) -> String {
        "NULL".to_string()
    }
}

/// Create a literal formatter from an adapter type
/// To be used internally for formatting literals in SQL
pub fn create_sql_literal_formatter(adapter_type: AdapterType) -> Box<dyn SqlLiteralFormatter> {
    match adapter_type {
        AdapterType::Postgres => Box::new(PostgreSqlLiteralFormatter {}),
        AdapterType::Snowflake => Box::new(SnowflakeSqlLiteralFormatter {}),
        AdapterType::Bigquery => Box::new(BigquerySqlLiteralFormatter {}),
        AdapterType::Databricks => Box::new(DatabricksSqlLiteralFormatter {}),
        AdapterType::Redshift => Box::new(RedshiftSqlLiteralFormatter {}),
        _ => unimplemented!("{} doesn't support a literal formatter", adapter_type),
    }
}
