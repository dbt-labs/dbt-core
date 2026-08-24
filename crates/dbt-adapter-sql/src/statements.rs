use dbt_adapter_core::AdapterType;

use crate::tokenizer::{Token, Tokenizer};

pub fn is_update_statement(sql: &str, adapter_type: AdapterType) -> bool {
    match adapter_type {
        AdapterType::ClickHouse => {
            let sql = trim_leading_sql_comments(sql);
            let mut tokenizer = Tokenizer::new(sql);
            matches!(
                tokenizer.next(),
                Some(Token::Word(token)) if !is_clickhouse_read_statement_token(token)
            )
        }
        AdapterType::Bigquery
        | AdapterType::Snowflake
        | AdapterType::Databricks
        | AdapterType::Redshift
        | AdapterType::Postgres
        | AdapterType::Salesforce
        | AdapterType::Spark
        | AdapterType::DuckDB
        | AdapterType::Fabric
        | AdapterType::Exasol
        | AdapterType::Starburst
        | AdapterType::Athena
        | AdapterType::Trino
        | AdapterType::Datafusion
        | AdapterType::Dremio
        | AdapterType::Oracle
        | AdapterType::LakeCompute => false,
    }
}

/// Whether `sql` is expected to produce a result set worth materializing.
///
/// BigQuery's ADBC driver Storage-Reads the destination table for DML when
/// `fetch=true` (e.g. `dbt.run_query` on `INSERT`). Callers must not drain
/// those readers. Leading comments are stripped so a comment prefix cannot
/// hide the statement keyword.
pub fn statement_returns_result_rows(sql: &str, adapter_type: AdapterType) -> bool {
    match adapter_type {
        AdapterType::Bigquery => {
            let sql = trim_leading_sql_comments(sql);
            let mut tokenizer = Tokenizer::new(sql);
            matches!(
                tokenizer.next(),
                Some(Token::Word(token)) if is_bigquery_result_statement_token(token)
            )
        }
        AdapterType::ClickHouse => !is_update_statement(sql, adapter_type),
        _ => true,
    }
}

/// BigQuery job `statistics.query.statementType` values that return rows.
pub fn bigquery_statement_type_returns_rows(statement_type: &str) -> bool {
    statement_type.eq_ignore_ascii_case("SELECT")
        || statement_type.eq_ignore_ascii_case("CALL")
        || statement_type.eq_ignore_ascii_case("SCRIPT")
}

fn is_bigquery_result_statement_token(token: &str) -> bool {
    // Conservative: only skip fetch when the first keyword cannot be a result
    // set. WITH starts CTEs that may SELECT; CALL/SCRIPT can return rows.
    token.eq_ignore_ascii_case("SELECT")
        || token.eq_ignore_ascii_case("WITH")
        || token.eq_ignore_ascii_case("SHOW")
        || token.eq_ignore_ascii_case("DESCRIBE")
        || token.eq_ignore_ascii_case("DESC")
        || token.eq_ignore_ascii_case("EXPLAIN")
        || token.eq_ignore_ascii_case("CALL")
}

fn trim_leading_sql_comments(mut sql: &str) -> &str {
    loop {
        let trimmed = sql.trim_start_matches(char::is_whitespace);
        if let Some(rest) = trimmed.strip_prefix("--") {
            match rest.split_once('\n') {
                Some((_, rest)) => {
                    sql = rest;
                    continue;
                }
                None => return "",
            }
        }
        if let Some(rest) = trimmed.strip_prefix("/*") {
            match rest.split_once("*/") {
                Some((_, rest)) => {
                    sql = rest;
                    continue;
                }
                None => return "",
            }
        }
        return trimmed;
    }
}

fn is_clickhouse_read_statement_token(token: &str) -> bool {
    // Keep this list in sync with the result-producing statements documented at
    // https://clickhouse.com/docs/sql-reference/statements. Misclassifying a
    // read statement as an update silently drops the result rows, so be
    // conservative and include every keyword that can return a result set.
    token.eq_ignore_ascii_case("SELECT")
        || token.eq_ignore_ascii_case("WITH")
        || token.eq_ignore_ascii_case("SHOW")
        || token.eq_ignore_ascii_case("DESCRIBE")
        || token.eq_ignore_ascii_case("DESC")
        || token.eq_ignore_ascii_case("EXPLAIN")
        || token.eq_ignore_ascii_case("EXISTS")
        || token.eq_ignore_ascii_case("CHECK")
        || token.eq_ignore_ascii_case("WATCH")
        || token.eq_ignore_ascii_case("KILL")
}

#[cfg(test)]
mod tests {
    use dbt_adapter_core::AdapterType;

    use super::{
        bigquery_statement_type_returns_rows, is_update_statement, statement_returns_result_rows,
    };

    #[test]
    fn clickhouse_update_statement_classification_uses_sql_tokenizer() {
        assert!(is_update_statement(
            "/* dbt */\nCREATE TABLE foo (id Int32)",
            AdapterType::ClickHouse,
        ));
        assert!(is_update_statement(
            "-- dbt\nINSERT INTO foo VALUES (1)",
            AdapterType::ClickHouse,
        ));
        assert!(!is_update_statement(
            "/* dbt */\nSELECT 1",
            AdapterType::ClickHouse,
        ));
        assert!(!is_update_statement("SHOW TABLES", AdapterType::ClickHouse));
        assert!(!is_update_statement(
            "DESC TABLE foo",
            AdapterType::ClickHouse,
        ));
        assert!(!is_update_statement(
            "DESCRIBE TABLE foo",
            AdapterType::ClickHouse,
        ));
        assert!(!is_update_statement(
            "WATCH live_view",
            AdapterType::ClickHouse,
        ));
        assert!(!is_update_statement(
            "KILL QUERY WHERE query_id = 'abc'",
            AdapterType::ClickHouse,
        ));
        assert!(!is_update_statement(
            "CREATE TABLE foo (id int)",
            AdapterType::DuckDB,
        ));
    }

    #[test]
    fn bigquery_insert_does_not_return_result_rows() {
        assert!(!statement_returns_result_rows(
            "INSERT INTO `proj`.`ds`.`target` (id) VALUES (1)",
            AdapterType::Bigquery,
        ));
    }

    #[test]
    fn bigquery_insert_with_leading_comment_does_not_return_result_rows() {
        let sql = "/* metadata */\ninsert into `proj`.`ds`.`target` values (1)";
        assert!(!statement_returns_result_rows(sql, AdapterType::Bigquery));
    }

    #[test]
    fn bigquery_dml_and_ddl_do_not_return_result_rows() {
        for sql in [
            "UPDATE t SET x = 1 WHERE true",
            "DELETE FROM t WHERE true",
            "MERGE t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET x = 1",
            "ALTER TABLE t SET OPTIONS (labels = [('a','b')])",
            "CREATE TABLE t AS SELECT 1 AS x",
            "DROP TABLE t",
            "TRUNCATE TABLE t",
        ] {
            assert!(
                !statement_returns_result_rows(sql, AdapterType::Bigquery),
                "expected no result rows for {sql}"
            );
        }
    }

    #[test]
    fn bigquery_select_and_with_return_result_rows() {
        assert!(statement_returns_result_rows(
            "SELECT 1",
            AdapterType::Bigquery,
        ));
        assert!(statement_returns_result_rows(
            "/* comment */\nSELECT table_name FROM `proj`.`ds`.INFORMATION_SCHEMA.TABLES",
            AdapterType::Bigquery,
        ));
        assert!(statement_returns_result_rows(
            "WITH cte AS (SELECT 1 AS x) SELECT * FROM cte",
            AdapterType::Bigquery,
        ));
        assert!(statement_returns_result_rows(
            "SHOW SCHEMAS",
            AdapterType::Bigquery,
        ));
    }

    #[test]
    fn statement_returns_result_rows_defaults_true_for_other_adapters() {
        assert!(statement_returns_result_rows(
            "INSERT INTO t VALUES (1)",
            AdapterType::Snowflake,
        ));
    }

    #[test]
    fn bigquery_statement_type_returns_rows_matches_job_statistics() {
        assert!(bigquery_statement_type_returns_rows("SELECT"));
        assert!(bigquery_statement_type_returns_rows("CALL"));
        assert!(bigquery_statement_type_returns_rows("SCRIPT"));
        assert!(!bigquery_statement_type_returns_rows("INSERT"));
        assert!(!bigquery_statement_type_returns_rows("MERGE"));
        assert!(!bigquery_statement_type_returns_rows("ALTER_TABLE"));
        assert!(!bigquery_statement_type_returns_rows(
            "CREATE_TABLE_AS_SELECT"
        ));
    }
}
