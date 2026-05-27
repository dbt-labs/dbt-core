use std::fmt::Debug;

use sqlparser::dialect::{
    BigQueryDialect, ClickHouseDialect, DatabricksDialect, Dialect, DuckDbDialect, GenericDialect,
    HiveDialect, MsSqlDialect, PostgreSqlDialect, RedshiftSqlDialect, SnowflakeDialect,
};
use sqlparser::tokenizer::{Location, Token, Tokenizer};

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

/// Implementation of [`StmtSplitter`] backed by the `sqlparser` crate's tokenizer.
#[derive(Debug)]
pub struct SqlparserStmtSplitter;

impl StmtSplitter for SqlparserStmtSplitter {
    fn split(&self, sql: &str, adapter_type: AdapterType) -> Vec<String> {
        // Match `sql_split_statements` in `dbt_sql_utils::splitter`: trim
        // leading/trailing whitespace so a trailing newline doesn't surface
        // as an extra whitespace-only statement after the last `;`.
        let sql = sql.trim();
        let dialect = sqlparser_dialect_for(adapter_type);
        let mut tokens = Vec::new();
        let aborted = Tokenizer::new(dialect, sql)
            .tokenize_with_location_into_buf(&mut tokens)
            .is_err();

        let mut cursor = LocationToByte::new(sql);
        let mut result = Vec::new();
        let mut start: Option<usize> = None;

        for t in &tokens {
            let tok_start = cursor.byte_of(t.span.start);
            if start.is_none() {
                start = Some(tok_start);
            }
            if matches!(t.token, Token::SemiColon) {
                let s = start.take().unwrap();
                result.push(sql[s..tok_start].to_string());
            }
        }

        if !aborted
            && let Some(s) = start
            && s < sql.len()
        {
            result.push(sql[s..].to_string());
        }

        result
    }

    fn is_empty(&self, sql: &str, adapter_type: AdapterType) -> bool {
        if sql.trim().is_empty() {
            return true;
        }
        let dialect = sqlparser_dialect_for(adapter_type);
        let mut tokens = Vec::new();
        // If tokenization fails, there is at least some non-whitespace content
        // (e.g. an unterminated quoted string), so it's not empty.
        if Tokenizer::new(dialect, sql)
            .tokenize_with_location_into_buf(&mut tokens)
            .is_err()
        {
            return false;
        }
        tokens
            .iter()
            .all(|t| matches!(t.token, Token::Whitespace(_)))
    }
}

/// Maps a dbt [`AdapterType`] to the closest `sqlparser` [`Dialect`].
///
/// Adapter types without a close match fall back to [`GenericDialect`].
fn sqlparser_dialect_for(adapter_type: AdapterType) -> &'static dyn Dialect {
    use AdapterType::*;
    static SNOWFLAKE: SnowflakeDialect = SnowflakeDialect {};
    static BIGQUERY: BigQueryDialect = BigQueryDialect {};
    static DATABRICKS: DatabricksDialect = DatabricksDialect {};
    static REDSHIFT: RedshiftSqlDialect = RedshiftSqlDialect {};
    static POSTGRES: PostgreSqlDialect = PostgreSqlDialect {};
    static DUCKDB: DuckDbDialect = DuckDbDialect {};
    static HIVE: HiveDialect = HiveDialect {};
    static MSSQL: MsSqlDialect = MsSqlDialect {};
    static CLICKHOUSE: ClickHouseDialect = ClickHouseDialect {};
    static GENERIC: GenericDialect = GenericDialect {};
    match adapter_type {
        Snowflake => &SNOWFLAKE,
        Bigquery => &BIGQUERY,
        Databricks => &DATABRICKS,
        Redshift => &REDSHIFT,
        Postgres => &POSTGRES,
        DuckDB => &DUCKDB,
        // Spark SQL is closest to Hive / Databricks; HiveDialect is a safe
        // baseline for tokenization (string/comment forms match).
        Spark => &HIVE,
        Fabric => &MSSQL,
        ClickHouse => &CLICKHOUSE,
        // No close sqlparser match — generic SQL tokenizer is permissive enough
        // for statement splitting.
        Trino | Athena | Starburst | Datafusion | Dremio | Oracle | Salesforce | Exasol => &GENERIC,
    }
}

/// Converts `sqlparser` (line, column) [`Location`]s to byte offsets in the
/// original input. `sqlparser` reports columns in `char`s, so this also
/// handles multi-byte UTF-8 correctly.
///
/// Requires that locations be queried in monotonically non-decreasing order,
/// which holds for tokens returned by the tokenizer.
struct LocationToByte<'a> {
    chars: std::str::CharIndices<'a>,
    input_len: usize,
    line: u64,
    col: u64,
    next_char: Option<(usize, char)>,
}

impl<'a> LocationToByte<'a> {
    fn new(input: &'a str) -> Self {
        let mut chars = input.char_indices();
        let next_char = chars.next();
        Self {
            chars,
            input_len: input.len(),
            line: 1,
            col: 1,
            next_char,
        }
    }

    fn byte_of(&mut self, loc: Location) -> usize {
        while (self.line, self.col) < (loc.line, loc.column) {
            match self.next_char {
                Some((_, c)) => {
                    if c == '\n' {
                        self.line += 1;
                        self.col = 1;
                    } else {
                        self.col += 1;
                    }
                    self.next_char = self.chars.next();
                }
                None => break,
            }
        }
        match self.next_char {
            Some((idx, _)) => idx,
            None => self.input_len,
        }
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
        SqlparserStmtSplitter.split(sql, adapter_type)
    }

    fn is_empty(sql: &str, adapter_type: AdapterType) -> bool {
        SqlparserStmtSplitter.is_empty(sql, adapter_type)
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
