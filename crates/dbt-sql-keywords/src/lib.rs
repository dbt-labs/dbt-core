mod generated;

pub mod bigquery;
pub mod databricks;
pub mod duckdb;
pub mod mssql;
pub mod redshift;
pub mod snowflake;
pub mod trino;

#[cfg(test)]
mod tests {
    use super::*;

    struct KeywordLists<'a> {
        pub name: &'static str,
        pub reserved: &'a [&'a str],
        pub strict_non_reserved: &'a [&'a str],
        pub non_reserved: &'a [&'a str],
    }

    static KEYWORD_LISTS: [KeywordLists; 7] = [
        KeywordLists {
            name: "bigquery",
            reserved: bigquery::RESERVED_KEYWORDS,
            strict_non_reserved: bigquery::STRICT_NON_RESERVED_KEYWORDS,
            non_reserved: bigquery::NON_RESERVED_KEYWORDS,
        },
        KeywordLists {
            name: "databricks",
            reserved: databricks::RESERVED_KEYWORDS,
            strict_non_reserved: databricks::STRICT_NON_RESERVED_KEYWORDS,
            non_reserved: databricks::NON_RESERVED_KEYWORDS,
        },
        KeywordLists {
            name: "duckdb",
            reserved: duckdb::RESERVED_KEYWORDS,
            strict_non_reserved: duckdb::STRICT_NON_RESERVED_KEYWORDS,
            non_reserved: duckdb::NON_RESERVED_KEYWORDS,
        },
        KeywordLists {
            name: "mssql",
            reserved: mssql::RESERVED_KEYWORDS,
            strict_non_reserved: mssql::STRICT_NON_RESERVED_KEYWORDS,
            non_reserved: mssql::NON_RESERVED_KEYWORDS,
        },
        KeywordLists {
            name: "redshift",
            reserved: redshift::RESERVED_KEYWORDS,
            strict_non_reserved: redshift::STRICT_NON_RESERVED_KEYWORDS,
            non_reserved: redshift::NON_RESERVED_KEYWORDS,
        },
        KeywordLists {
            name: "snowflake",
            reserved: snowflake::RESERVED_KEYWORDS,
            strict_non_reserved: snowflake::STRICT_NON_RESERVED_KEYWORDS,
            non_reserved: snowflake::NON_RESERVED_KEYWORDS,
        },
        KeywordLists {
            name: "trino",
            reserved: trino::RESERVED_KEYWORDS,
            strict_non_reserved: trino::STRICT_NON_RESERVED_KEYWORDS,
            non_reserved: trino::NON_RESERVED_KEYWORDS,
        },
    ];

    fn assert_sorted(name: &str, list: &[&str]) {
        for i in 1..list.len() {
            assert!(
                list[i - 1] < list[i],
                "{name} is not sorted: '{}' >= '{}'",
                list[i - 1],
                list[i]
            );
        }
    }

    #[test]
    fn test_keywords_are_sorted() {
        for lists in &KEYWORD_LISTS {
            let name = format!("{}.{}", lists.name, "RESERVED_KEYWORDS");
            assert_sorted(&name, lists.reserved);
            let name = format!("{}.{}", lists.name, "STRICT_NON_RESERVED_KEYWORDS");
            assert_sorted(&name, lists.strict_non_reserved);
            let name = format!("{}.{}", lists.name, "NON_RESERVED_KEYWORDS");
            assert_sorted(&name, lists.non_reserved);
        }
    }
}
