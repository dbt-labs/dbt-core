use crate::formatter::SqlLiteralFormatter;

pub struct DatabricksSqlLiteralFormatter;

impl SqlLiteralFormatter for DatabricksSqlLiteralFormatter {
    // https://docs.databricks.com/aws/en/sql/language-manual/data-types/string-type
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
        let formatter = DatabricksSqlLiteralFormatter;

        assert_eq!(formatter.format_str("hello"), "'hello'");

        assert_eq!(formatter.format_str("it's"), "'it\\'s'");

        assert_eq!(formatter.format_str("it's a test's"), "'it\\'s a test\\'s'");

        assert_eq!(formatter.format_str(""), "''");

        assert_eq!(formatter.format_str("\\"), "'\\'");

        assert_eq!(formatter.format_str("\\'"), "'\\\\''");
    }
}
