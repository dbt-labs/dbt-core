// This file contains lists of reserved keywords for various SQL dialects.
//
// All keywords are in uppercase and sorted so that binary search can be used
// to check for membership.

use dbt_xdbc::Backend;

pub fn sorted_keywords_for(backend: Backend) -> &'static [&'static str] {
    use Backend::*;
    match backend {
        Snowflake => SNOWFLAKE_RESERVED_KEYWORDS,
        BigQuery => BIGQUERY_RESERVED_KEYWORDS,
        Redshift | RedshiftODBC => REDSHIFT_RESERVED_KEYWORDS,
        // TODO: fill in other dialects' keywords and define a default fallback
        Databricks | DatabricksODBC | Postgres | Salesforce | Generic { .. } => &[],
    }
}

/// Compares an uppercase keyword with a token in a case-insensitive manner.
///
/// This function exists because we don't want to heap-allocate a new uppercase
/// string for every token we want to check.
///
/// PRE-CONDITION: `kw` is uppercase.
fn keyword_cmp_ignore_ascii_case(kw: &str, token: &str) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    let mut a = kw.as_bytes();
    let mut b = token.as_bytes();
    while let ([first_a, rest_a @ ..], [first_b, rest_b @ ..]) = (a, b) {
        // first_a is already uppercase because kw is uppercase
        match first_a.cmp(&first_b.to_ascii_uppercase()) {
            Less => return Less,
            Greater => return Greater,
            Equal => {
                a = rest_a;
                b = rest_b;
            }
        }
    }
    a.len().cmp(&b.len())
}

/// Returns the uppercase version of the given token if it is a reserved keyword.
pub fn is_keyword_ignore_ascii_case(backend: Backend, token: &str) -> Option<&'static str> {
    let sorted_keywords = sorted_keywords_for(backend);
    sorted_keywords
        .binary_search_by(|kw| keyword_cmp_ignore_ascii_case(kw, token))
        .ok()
        .map(|idx| sorted_keywords[idx])
}

static BIGQUERY_RESERVED_KEYWORDS: &[&str] = &[
    "ALL",
    "AND",
    "ANY",
    "ARRAY",
    "AS",
    "ASC",
    "AT",
    "BETWEEN",
    "BY",
    "CASE",
    "CAST",
    "COLLATE",
    "CREATE",
    "CROSS",
    "CUBE",
    "CURRENT",
    "DEFAULT",
    "DEFINE",
    "DESC",
    "DISTINCT",
    "ELSE",
    "END",
    "ESCAPE",
    "EXCEPT",
    "EXCLUDE",
    "EXISTS",
    "EXTRACT",
    "FALSE",
    "FETCH",
    "FOLLOWING",
    "FOR",
    "FROM",
    "FULL",
    "GROUP",
    "GROUPING",
    "GROUPS",
    "HAVING",
    "IF",
    "IGNORE",
    "IN",
    "INNER",
    "INTERSECT",
    "INTERVAL",
    "INTO",
    "IS",
    "JOIN",
    "LATERAL",
    "LEFT",
    "LIKE",
    "LIMIT",
    "MATCH_RECOGNIZE",
    "MERGE",
    "NATURAL",
    "NO",
    "NOT",
    "NULL",
    "NULLS",
    "OF",
    "ON",
    "OR",
    "ORDER",
    "OUTER",
    "OVER",
    "PARTITION",
    "PRECEDING",
    "QUALIFY",
    "RANGE",
    "RECURSIVE",
    "RESPECT",
    "RIGHT",
    "ROLLUP",
    "ROWS",
    "SELECT",
    "SET",
    "SOME",
    "STRUCT",
    "TABLESAMPLE",
    "THEN",
    "TO",
    "TRUE",
    "UNBOUNDED",
    "UNION",
    "UNNEST",
    "USING",
    "WHEN",
    "WHERE",
    "WINDOW",
    "WITH",
];

static REDSHIFT_RESERVED_KEYWORDS: &[&str] = &[
    "AS", "IDENTITY", "SNAPSHOT", "SYSTEM", "TOP", "UNLOAD", "WITHIN",
];

static SNOWFLAKE_RESERVED_KEYWORDS: &[&str] = &[
    "ALL",
    "ALTER",
    "AND",
    "ANY",
    "AS",
    "BETWEEN",
    "BY",
    "COLUMN",
    "CONNECT",
    "CREATE",
    "CURRENT",
    "DELETE",
    "DISTINCT",
    "DROP",
    "ELSE",
    "EXISTS",
    "FOLLOWING",
    "FOR",
    "FROM",
    "GRANT",
    "GROUP",
    "HAVING",
    "ILIKE",
    "IN",
    "INCREMENT",
    "INSERT",
    "INTERSECT",
    "INTO",
    "IS",
    "LIKE",
    "MINUS",
    "NOT",
    "NULL",
    "OF",
    "ON",
    "OR",
    "ORDER",
    "QUALIFY",
    "REGEXP",
    "REVOKE",
    "RLIKE",
    "ROW",
    "ROWS",
    "SAMPLE",
    "SELECT",
    "SET",
    "SOME",
    "START",
    "TABLE",
    "TABLESAMPLE",
    "THEN",
    "TO",
    "UNION",
    "UNIQUE",
    "UPDATE",
    "VALUES",
    "WHERE",
    "WITH",
];

#[allow(dead_code)]
static TRINO_RESERVED_KEYWORDS: &[&str] = &[
    "ALTER",
    "AND",
    "AS",
    "AUTO",
    "BACKUP",
    "BETWEEN",
    "BY",
    "CASE",
    "CASE_INSENSITIVE",
    "CASE_SENSITIVE",
    "CAST",
    "COLLATE",
    "COMPOUND",
    "CONNECT",
    "CONSTRAINT",
    "CREATE",
    "CROSS",
    "CUBE",
    "CURRENT_ROLE",
    "DEALLOCATE",
    "DEFAULTS",
    "DELETE",
    "DELIMITED",
    "DISTINCT",
    "DISTKEY",
    "DISTSTYLE",
    "DROP",
    "ELSE",
    "ENCODE",
    "ESCAPE",
    "EVEN",
    "EXCEPT",
    "EXISTS",
    "EXTRACT",
    "FALSE",
    "FOR",
    "FROM",
    "FULL",
    "FUNCTION",
    "GENERATED",
    "GROUP",
    "GROUPING",
    "HAVING",
    "HEADER",
    "IDENTITY",
    "ILIKE",
    "IN",
    "INNER",
    "INPUTFORMAT",
    "INSERT",
    "INTERLEAVED",
    "INTERSECT",
    "INTO",
    "IS",
    "JOIN",
    "JSON_ARRAY",
    "JSON_EXISTS",
    "JSON_OBJECT",
    "JSON_QUERY",
    "JSON_VALUE",
    "LEFT",
    "LIKE",
    "LISTAGG",
    "NATURAL",
    "NORMALIZE",
    "NOT",
    "NULL",
    "ON",
    "OPTIONS",
    "OR",
    "ORDER",
    "OUTER",
    "OUTPUTFORMAT",
    "PARTITIONED",
    "PREPARE",
    "PRIOR",
    "RECURSIVE",
    "RIGHT",
    "SELECT",
    "SERDE",
    "SERDEPROPERTIES",
    "SIMILAR",
    "SKIP",
    "SORTKEY",
    "STORED",
    "TABLE",
    "TERMINATED",
    "THEN",
    "TOP",
    "TRIM",
    "TRUE",
    "UESCAPE",
    "UNION",
    "UNNEST",
    "UNSIGNED",
    "USING",
    "VALUES",
    "WHEN",
    "WHERE",
    "WITH",
    "YES",
];

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_is_sorted(keywords: &[&str]) {
        for i in 1..keywords.len() {
            assert!(
                keywords[i - 1] < keywords[i],
                "Keyword list is not sorted: '{}' should come before '{}'",
                keywords[i - 1],
                keywords[i]
            );
        }
    }

    #[test]
    fn test_bigquery_keywords_sorted() {
        assert_is_sorted(BIGQUERY_RESERVED_KEYWORDS);
    }

    #[test]
    fn test_redshift_keywords_sorted() {
        assert_is_sorted(REDSHIFT_RESERVED_KEYWORDS);
    }

    #[test]
    fn test_snowflake_keywords_sorted() {
        assert_is_sorted(SNOWFLAKE_RESERVED_KEYWORDS);
    }

    #[test]
    fn test_trino_keywords_sorted() {
        assert_is_sorted(TRINO_RESERVED_KEYWORDS);
    }

    #[test]
    fn test_keyword_cmp_ignore_ascii_case() {
        use std::cmp::Ordering::*;
        assert_eq!(keyword_cmp_ignore_ascii_case("SELECT", "select"), Equal);
        assert_eq!(keyword_cmp_ignore_ascii_case("SELECT", "SeLeCt"), Equal);
        assert_eq!(keyword_cmp_ignore_ascii_case("SELECT", "SELECTED"), Less);
        assert_eq!(keyword_cmp_ignore_ascii_case("SELECT", "SEL"), Greater);
        assert_eq!(keyword_cmp_ignore_ascii_case("SELECT", "ASELECT"), Greater);
        assert_eq!(keyword_cmp_ignore_ascii_case("SELECT", "ZSELECT"), Less);
    }

    fn is_kw(token: &str) -> Option<&'static str> {
        is_keyword_ignore_ascii_case(Backend::BigQuery, token)
    }

    #[test]
    fn test_is_keyword_ignore_ascii_case() {
        assert_eq!(is_kw("select"), Some("SELECT"));
        assert_eq!(is_kw("SeLeCt"), Some("SELECT"));
        assert_eq!(is_kw("SELECTED"), None);
        assert_eq!(is_kw("SEL"), None);
        assert_eq!(is_kw("ASELECT"), None);
        assert_eq!(is_kw("ZSELECT"), None);
        assert_eq!(is_kw("null"), Some("NULL"));
        assert_eq!(is_kw("NULLs"), Some("NULLS"));
        assert_eq!(is_kw("nulos"), None);
        for kw in BIGQUERY_RESERVED_KEYWORDS {
            assert_eq!(is_kw(kw), Some(*kw));
            assert_eq!(is_kw(kw.to_ascii_lowercase().as_str()), Some(*kw));
            let not_kw = format!("X{kw}");
            assert_eq!(is_kw(&not_kw), None);
            let not_kw = format!("{kw}X");
            assert_eq!(is_kw(&not_kw), None);
            let not_kw = format!("☃{kw}☃");
            assert_eq!(is_kw(&not_kw), None);
        }
    }
}
