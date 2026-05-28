use super::*;
use dbt_frontend_common::dialect::Dialect;

#[test]
fn test_sql_split_statements() {
    // Test basic splitting - no filtering happens
    assert_eq!(do_sql_split_statements("", None), Vec::<&str>::new());
    assert_eq!(
        do_sql_split_statements("SELECT 1; SELECT 2; SELECT 3;", None),
        vec!["SELECT 1", " SELECT 2", " SELECT 3"]
    );

    // Empty statements are NOT filtered
    assert_eq!(do_sql_split_statements(";;;", None), vec!["", "", ""]);

    // Comments are NOT filtered (filtering happens in adapter layer)
    assert_eq!(
        do_sql_split_statements("select 1; /* end comment */", None),
        vec!["select 1", " /* end comment */"]
    );
    assert_eq!(
        do_sql_split_statements("select 1; -- line comment", None),
        vec!["select 1", " -- line comment"]
    );

    // Statements with embedded comments are kept as-is
    assert_eq!(
        do_sql_split_statements("/* before */ select 1 /* after */", None),
        vec!["/* before */ select 1 /* after */"]
    );
}

#[test]
fn test_is_empty_or_comment_only() {
    // Test the comment detection helper function across all dialects
    let all_dialects = vec![
        Some(Dialect::Snowflake),
        Some(Dialect::Bigquery),
        Some(Dialect::Redshift),
        Some(Dialect::Databricks),
        Some(Dialect::Trino),
        None,
    ];

    for dialect in &all_dialects {
        // These should be considered empty/comment-only
        assert!(is_empty_or_comment_only("", *dialect));
        assert!(is_empty_or_comment_only("   ", *dialect));
        assert!(is_empty_or_comment_only("/* comment */", *dialect));
        assert!(is_empty_or_comment_only("-- line comment", *dialect));
        assert!(is_empty_or_comment_only("  /* comment */  ", *dialect));
        assert!(is_empty_or_comment_only("  -- comment  ", *dialect));
        assert!(is_empty_or_comment_only(
            "/* comment */ -- line comment",
            *dialect
        ));
        assert!(is_empty_or_comment_only(
            "/* multi\nline\ncomment */",
            *dialect
        ));

        // These should NOT be considered empty - SQL with comments should be preserved
        assert!(!is_empty_or_comment_only("select 1", *dialect));
        assert!(!is_empty_or_comment_only(
            "select /* comment */ 1",
            *dialect
        ));
        assert!(!is_empty_or_comment_only("select 1 -- comment", *dialect));
        assert!(!is_empty_or_comment_only(
            "/* comment */ select 1",
            *dialect
        ));

        // Additional critical cases that should NOT be filtered
        assert!(!is_empty_or_comment_only(
            "/* before */ select 1 /* after */",
            *dialect
        ));
        assert!(!is_empty_or_comment_only("-- comment\nselect 1", *dialect));
        assert!(!is_empty_or_comment_only("select 1; select 2", *dialect));
        assert!(!is_empty_or_comment_only(
            "/* comment */\nselect 1\n-- trailing",
            *dialect
        ));
    }
}
