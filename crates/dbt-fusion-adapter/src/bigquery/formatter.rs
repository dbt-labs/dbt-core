use crate::formatter::SqlLiteralFormatter;

pub struct BigquerySqlLiteralFormatter;

impl SqlLiteralFormatter for BigquerySqlLiteralFormatter {
    // bigquery uses \ for string escapes
    fn format_str(&self, l: &str) -> String {
        let escaped_str = l.replace("'", "\\'");
        format!("'{escaped_str}'")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_str() {
        let formatter = BigquerySqlLiteralFormatter;

        assert_eq!(formatter.format_str("hello"), "'hello'");

        assert_eq!(formatter.format_str("it's"), "'it\\'s'");

        assert_eq!(formatter.format_str("it's a test's"), "'it\\'s a test\\'s'");

        assert_eq!(formatter.format_str(""), "''");

        assert_eq!(formatter.format_str("\\"), "'\\'");

        assert_eq!(formatter.format_str("\\'"), "'\\\\''");
    }
}
