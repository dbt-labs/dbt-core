use std::fmt::Debug;

use dbt_common::adapter::dialect_of;
use dbt_sql_utils::{is_empty_or_comment_only, sql_split_statements};

use crate::AdapterType;

/// Trait for SQL statement splitting functionality
pub trait StmtSplitter: Send + Sync + Debug {
    /// Split a SQL string into individual statements
    ///
    /// The implementation should:
    /// - Split the SQL into individual statements based on delimiters
    /// - Handle dialect-specific syntax correctly
    fn split(&self, sql: &str, adapter_type: AdapterType) -> Vec<String>;

    /// Determine if a SQL string is either empty or only contains a comment
    fn is_empty(&self, sql: &str, adapter_type: AdapterType) -> bool;
}

#[derive(Debug)]
pub struct DefaultStmtSplitter;

impl StmtSplitter for DefaultStmtSplitter {
    fn split(&self, sql: &str, adapter_type: AdapterType) -> Vec<String> {
        let dialect = dialect_of(adapter_type);
        // Use sql_split_statements for splitting, then filter out empty/comment-only statements
        // This separation of concerns keeps filtering logic in the adapter layer
        sql_split_statements(sql, dialect).into_iter().collect()
    }

    fn is_empty(&self, sql: &str, adapter_type: AdapterType) -> bool {
        let dialect = dialect_of(adapter_type);
        is_empty_or_comment_only(sql, dialect)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The splitter is dialect-aware only insofar as the tokenizer is. For
    // most cases the result is identical across dialects, so we run the
    // common cases against this representative set (one variant per
    // sqlparser dialect we map to, plus Trino which routes to Generic).
    const REPRESENTATIVE_DIALECTS: &[AdapterType] = &[
        AdapterType::Snowflake,
        AdapterType::Bigquery,
        AdapterType::Redshift,
        AdapterType::Databricks,
        AdapterType::Postgres,
        AdapterType::DuckDB,
        AdapterType::Spark,
        AdapterType::Fabric,
        AdapterType::ClickHouse,
        AdapterType::Trino,
    ];

    fn split(sql: &str, adapter_type: AdapterType) -> Vec<String> {
        DefaultStmtSplitter.split(sql, adapter_type)
    }

    fn is_empty(sql: &str, adapter_type: AdapterType) -> bool {
        DefaultStmtSplitter.is_empty(sql, adapter_type)
    }

    // ---- split: ported from dbt_sql_utils::splitter::tests ----

    #[test]
    fn test_split_basic() {
        for d in REPRESENTATIVE_DIALECTS {
            assert_eq!(split("", *d), Vec::<String>::new());
            assert_eq!(
                split("SELECT 1; SELECT 2; SELECT 3;", *d),
                vec!["SELECT 1", " SELECT 2", " SELECT 3"]
            );
        }
    }

    #[test]
    fn test_split_empty_statements_not_filtered() {
        for d in REPRESENTATIVE_DIALECTS {
            assert_eq!(split(";;;", *d), vec!["", "", ""]);
        }
    }

    #[test]
    fn test_split_comments_not_filtered() {
        for d in REPRESENTATIVE_DIALECTS {
            assert_eq!(
                split("select 1; /* end comment */", *d),
                vec!["select 1", " /* end comment */"]
            );
            assert_eq!(
                split("select 1; -- line comment", *d),
                vec!["select 1", " -- line comment"]
            );
        }
    }

    #[test]
    fn test_split_statement_with_embedded_comments() {
        for d in REPRESENTATIVE_DIALECTS {
            assert_eq!(
                split("/* before */ select 1 /* after */", *d),
                vec!["/* before */ select 1 /* after */"]
            );
        }
    }

    // ---- split: behavior unique to a real lexer (vs NaiveStmtSplitter) ----

    #[test]
    fn test_split_semicolon_in_string_literal() {
        for d in REPRESENTATIVE_DIALECTS {
            assert_eq!(
                split("select 'a;b'; select 2", *d),
                vec!["select 'a;b'", " select 2"]
            );
        }
    }

    #[test]
    fn test_split_semicolon_in_block_comment() {
        // Mirrors the existing SdfStmtSplitter test.
        assert_eq!(
            split(
                "select 1; /* comment with ; */; select 2",
                AdapterType::Snowflake
            ),
            vec!["select 1", " /* comment with ; */", " select 2"]
        );
    }

    #[test]
    fn test_split_semicolon_in_line_comment() {
        for d in REPRESENTATIVE_DIALECTS {
            assert_eq!(
                split("select 1 -- trailing ; comment\n; select 2", *d),
                vec!["select 1 -- trailing ; comment\n", " select 2"]
            );
        }
    }

    /// Regression for https://github.com/dbt-labs/dbt-fusion/issues/1031:
    /// Redshift `persist_docs` generates multiple `COMMENT ON COLUMN` statements
    /// using dollar-quoted strings. Embedded `;` inside `$tag$...$tag$` must
    /// not split a statement.
    #[test]
    fn test_split_redshift_dollar_quoted_strings() {
        let sql = r#"
    comment on column "ci"."fusion_tests_schema"."test_model".id is $dbt_comment_literal_block$The unique identifier$dbt_comment_literal_block$;
    comment on column "ci"."fusion_tests_schema"."test_model".name is $dbt_comment_literal_block$The person's name$dbt_comment_literal_block$;
    comment on column "ci"."fusion_tests_schema"."test_model".age is $dbt_comment_literal_block$The person's age$dbt_comment_literal_block$;
    comment on column "ci"."fusion_tests_schema"."test_model".department is $dbt_comment_literal_block$The person's department$dbt_comment_literal_block$;
  "#;

        let statements = split(sql, AdapterType::Redshift);

        assert_eq!(
            statements.len(),
            4,
            "Expected 4 COMMENT ON COLUMN statements, got {}: {:?}",
            statements.len(),
            statements
        );
        for stmt in &statements {
            assert!(
                stmt.trim().to_lowercase().starts_with("comment on column"),
                "Expected COMMENT ON COLUMN statement, got: {stmt}"
            );
        }
    }

    #[test]
    fn test_split_unterminated_string_drops_partial_trailing() {
        // The tokenizer aborts on the unterminated string; we keep only the
        // clean prefix, matching the UNPAIRED_TOKEN behavior of the sdf splitter.
        let result = split("select 1; select 'unterminated", AdapterType::Snowflake);
        assert_eq!(result, vec!["select 1"]);
    }

    // ---- is_empty: ported from is_empty_or_comment_only ----

    #[test]
    fn test_is_empty_comment_or_whitespace_only() {
        for d in REPRESENTATIVE_DIALECTS {
            assert!(is_empty("", *d));
            assert!(is_empty("   ", *d));
            assert!(is_empty("/* comment */", *d));
            assert!(is_empty("-- line comment", *d));
            assert!(is_empty("  /* comment */  ", *d));
            assert!(is_empty("  -- comment  ", *d));
            assert!(is_empty("/* comment */ -- line comment", *d));
            assert!(is_empty("/* multi\nline\ncomment */", *d));
        }
    }

    #[test]
    fn test_is_empty_with_sql_content() {
        for d in REPRESENTATIVE_DIALECTS {
            assert!(!is_empty("select 1", *d));
            assert!(!is_empty("select /* comment */ 1", *d));
            assert!(!is_empty("select 1 -- comment", *d));
            assert!(!is_empty("/* comment */ select 1", *d));
            assert!(!is_empty("/* before */ select 1 /* after */", *d));
            assert!(!is_empty("-- comment\nselect 1", *d));
            assert!(!is_empty("select 1; select 2", *d));
            assert!(!is_empty("/* comment */\nselect 1\n-- trailing", *d));
        }
    }
}
