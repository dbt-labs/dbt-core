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
        | AdapterType::Oracle => false,
    }
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

    use super::is_update_statement;

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
}
