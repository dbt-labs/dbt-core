use std::collections::HashSet;

use dbt_adapter_core::AdapterType;
use once_cell::sync::Lazy;

pub fn format_ident(id: &str, adapter: AdapterType) -> String {
    if need_quotes(id, adapter) {
        match adapter {
            AdapterType::Fabric => format!("[{}]", id),
            _ => format!("\"{}\"", id),
        }
    } else {
        id.to_string()
    }
}

pub fn need_quotes(id: &str, adapter: AdapterType) -> bool {
    if id.is_empty() {
        return true;
    }

    let mut chars = id.chars();

    // First character rules
    let first_char = chars.next().unwrap();
    let valid_start = match adapter {
        AdapterType::Fabric => {
            first_char == '_'
                || first_char == '@'
                || first_char == '#'
                || first_char.is_ascii_alphabetic()
        }
        _ => first_char == '_' || first_char.is_ascii_alphabetic(),
    };

    if !valid_start {
        return true;
    }

    // Remaining characters
    if chars.any(|c| !is_valid_identifier_char(adapter, c)) {
        return true;
    }

    // Reserved keywords (simple baseline)
    let reserved = reserved_keywords(adapter);
    if reserved.contains(&id.to_ascii_uppercase().as_str()) {
        return true;
    }

    // Optional: T-SQL / Fabric identifier length limit
    if matches!(adapter, AdapterType::Fabric) && id.len() > 128 {
        return true;
    }

    false
}

fn is_valid_identifier_char(adapter: AdapterType, c: char) -> bool {
    match adapter {
        AdapterType::Fabric => c.is_alphanumeric() || c == '_',
        _ => c.is_alphanumeric() || c == '_',
    }
}

pub fn reserved_keywords(adapter: AdapterType) -> &'static HashSet<&'static str> {
    match adapter {
        AdapterType::Fabric => &FABRIC_KEYWORDS,
        _ => unimplemented!("Reserved keyword unimplemented for adapter {}", adapter),
    }
}

//TODO: revisit after Dialect is added for TSQL
static FABRIC_KEYWORDS: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    HashSet::from([
        "ADD",
        "EXTERNAL",
        "PROCEDURE",
        "ALL",
        "FETCH",
        "PUBLIC",
        "ALTER",
        "FILE",
        "RAISERROR",
        "AND",
        "FILLFACTOR",
        "READ",
        "ANY",
        "FOR",
        "READTEXT",
        "AS",
        "FOREIGN",
        "RECONFIGURE",
        "ASC",
        "FREETEXT",
        "REFERENCES",
        "AUTHORIZATION",
        "FREETEXTTABLE",
        "REPLICATION",
        "BACKUP",
        "FROM",
        "RESTORE",
        "BEGIN",
        "FULL",
        "RESTRICT",
        "BETWEEN",
        "FUNCTION",
        "RETURN",
        "BREAK",
        "GOTO",
        "REVERT",
        "BROWSE",
        "GRANT",
        "REVOKE",
        "BULK",
        "GROUP",
        "RIGHT",
        "BY",
        "HAVING",
        "ROLLBACK",
        "CASCADE",
        "HOLDLOCK",
        "ROWCOUNT",
        "CASE",
        "IDENTITY",
        "ROWGUIDCOL",
        "CHECK",
        "IDENTITY_INSERT",
        "RULE",
        "CHECKPOINT",
        "IDENTITYCOL",
        "SAVE",
        "CLOSE",
        "IF",
        "SCHEMA",
        "CLUSTERED",
        "IN",
        "SECURITYAUDIT",
        "COALESCE",
        "INDEX",
        "SELECT",
        "COLLATE",
        "INNER",
        "SEMANTICKEYPHRASETABLE",
        "COLUMN",
        "INSERT",
        "SEMANTICSIMILARITYDETAILSTABLE",
        "COMMIT",
        "INTERSECT",
        "SEMANTICSIMILARITYTABLE",
        "COMPUTE",
        "INTO",
        "SESSION_USER",
        "CONSTRAINT",
        "IS",
        "SET",
        "CONTAINS",
        "JOIN",
        "SETUSER",
        "CONTAINSTABLE",
        "KEY",
        "SHUTDOWN",
        "CONTINUE",
        "KILL",
        "SOME",
        "CONVERT",
        "LEFT",
        "STATISTICS",
        "CREATE",
        "LIKE",
        "SYSTEM_USER",
        "CROSS",
        "LINENO",
        "TABLE",
        "CURRENT",
        "LOAD",
        "TABLESAMPLE",
        "CURRENT_DATE",
        "MERGE",
        "TEXTSIZE",
        "CURRENT_TIME",
        "NATIONAL",
        "THEN",
        "CURRENT_TIMESTAMP",
        "NOCHECK",
        "TO",
        "CURRENT_USER",
        "NONCLUSTERED",
        "TOP",
        "CURSOR",
        "NOT",
        "TRAN",
        "DATABASE",
        "NULL",
        "TRANSACTION",
        "DBCC",
        "NULLIF",
        "TRIGGER",
        "DEALLOCATE",
        "OF",
        "TRUNCATE",
        "DECLARE",
        "OFF",
        "TRY_CONVERT",
        "DEFAULT",
        "OFFSETS",
        "TSEQUAL",
        "DELETE",
        "ON",
        "UNION",
        "DENY",
        "OPEN",
        "UNIQUE",
        "DESC",
        "OPENDATASOURCE",
        "UNPIVOT",
        "DISK",
        "OPENQUERY",
        "UPDATE",
        "DISTINCT",
        "OPENROWSET",
        "UPDATETEXT",
        "DISTRIBUTED",
        "OPENXML",
        "USE",
        "DOUBLE",
        "OPTION",
        "USER",
        "DROP",
        "OR",
        "VALUES",
        "DUMP",
        "ORDER",
        "VARYING",
        "ELSE",
        "OUTER",
        "VIEW",
        "END",
        "OVER",
        "WAITFOR",
        "ERRLVL",
        "PERCENT",
        "WHEN",
        "ESCAPE",
        "PIVOT",
        "WHERE",
        "EXCEPT",
        "PLAN",
        "WHILE",
        "EXEC",
        "PRECISION",
        "WITH",
        "EXECUTE",
        "PRIMARY",
        "WITHIN GROUP",
        "EXISTS",
        "PRINT",
        "WRITETEXT",
        "EXIT",
        "PROC",
        "LABEL", // Synapse-exclusive
        // ISO / ODBC reserved keywords
        "ABSOLUTE",
        "EXECUTE",
        "OVERLAPS",
        "ACTION",
        "PAD",
        "ADA",
        "PARTIAL",
        "PASCAL",
        "EXTRACT",
        "POSITION",
        "ALLOCATE",
        "FALSE",
        "PRECISION",
        "FIRST",
        "PRESERVE",
        "FLOAT",
        "PRIOR",
        "ARE",
        "FOR",
        "PRIVILEGES",
        "ASSERTION",
        "FOUND",
        "PUBLIC",
        "AT",
        "FROM",
        "READ",
        "AUTHORIZATION",
        "FULL",
        "REAL",
        "AVG",
        "GET",
        "REFERENCES",
        "BEGIN",
        "GLOBAL",
        "RELATIVE",
        "BETWEEN",
        "GO",
        "RESTRICT",
        "BIT",
        "GOTO",
        "REVOKE",
        "BIT_LENGTH",
        "GRANT",
        "RIGHT",
        "BOTH",
        "GROUP",
        "ROLLBACK",
        "BY",
        "HAVING",
        "ROWS",
        "CASCADE",
        "HOUR",
        "SCHEMA",
        "CASCADED",
        "IDENTITY",
        "SCROLL",
        "CASE",
        "IMMEDIATE",
        "SECOND",
        "CAST",
        "IN",
        "SECTION",
        "CATALOG",
        "INCLUDE",
        "SELECT",
        "CHAR",
        "INDEX",
        "SESSION",
        "CHAR_LENGTH",
        "INDICATOR",
        "SESSION_USER",
        "CHARACTER",
        "INITIALLY",
        "SET",
        "CHARACTER_LENGTH",
        "INNER",
        "SIZE",
        "CHECK",
        "INPUT",
        "SMALLINT",
        "CLOSE",
        "INSENSITIVE",
        "SOME",
        "COALESCE",
        "INSERT",
        "SPACE",
        "COLLATE",
        "INT",
        "SQL",
        "COLLATION",
        "INTEGER",
        "SQLCA",
        "COLUMN",
        "INTERSECT",
        "SQLCODE",
        "COMMIT",
        "INTERVAL",
        "SQLERROR",
        "CONNECT",
        "INTO",
        "SQLSTATE",
        "CONNECTION",
        "IS",
        "SQLWARNING",
        "CONSTRAINT",
        "ISOLATION",
        "SUBSTRING",
        "CONSTRAINTS",
        "JOIN",
        "SUM",
        "CONTINUE",
        "KEY",
        "SYSTEM_USER",
        "CONVERT",
        "LANGUAGE",
        "TABLE",
        "CORRESPONDING",
        "LAST",
        "TEMPORARY",
        "COUNT",
        "LEADING",
        "THEN",
        "CREATE",
        "LEFT",
        "TIME",
        "CROSS",
        "LEVEL",
        "TIMESTAMP",
        "CURRENT",
        "LIKE",
        "TIMEZONE_HOUR",
        "CURRENT_DATE",
        "LOCAL",
        "TIMEZONE_MINUTE",
        "CURRENT_TIME",
        "LOWER",
        "TO",
        "CURRENT_TIMESTAMP",
        "MATCH",
        "TRAILING",
        "CURRENT_USER",
        "MAX",
        "TRANSACTION",
        "CURSOR",
        "MIN",
        "TRANSLATE",
        "DATE",
        "MINUTE",
        "TRANSLATION",
        "DAY",
        "MODULE",
        "TRIM",
        "DEALLOCATE",
        "MONTH",
        "TRUE",
        "DEC",
        "NAMES",
        "UNION",
        "DECIMAL",
        "NATIONAL",
        "UNIQUE",
        "DECLARE",
        "NATURAL",
        "UNKNOWN",
        "DEFAULT",
        "NCHAR",
        "UPDATE",
        "DEFERRABLE",
        "NEXT",
        "UPPER",
        "DEFERRED",
        "NO",
        "USAGE",
        "DELETE",
        "NONE",
        "USER",
        "DESC",
        "NOT",
        "USING",
        "DESCRIBE",
        "NULL",
        "VALUE",
        "DESCRIPTOR",
        "NULLIF",
        "VALUES",
        "DIAGNOSTICS",
        "NUMERIC",
        "VARCHAR",
        "DISCONNECT",
        "OCTET_LENGTH",
        "VARYING",
        "DISTINCT",
        "OF",
        "VIEW",
        "DOMAIN",
        "ON",
        "WHEN",
        "DOUBLE",
        "ONLY",
        "WHENEVER",
        "DROP",
        "OPEN",
        "WHERE",
        "ELSE",
        "OPTION",
        "WITH",
        "END",
        "OR",
        "WORK",
        "END-EXEC",
        "ORDER",
        "WRITE",
        "ESCAPE",
        "OUTER",
        "YEAR",
        "EXCEPT",
        "OUTPUT",
        "ZONE",
    ])
});

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_adapter_core::AdapterType;

    #[test]
    fn test_format_ident_unquoted() {
        let id = "my_column";
        let formatted = format_ident(id, AdapterType::Fabric);
        assert_eq!(formatted, "my_column");
    }

    #[test]
    fn test_format_ident_quoted_fabric() {
        let id = "select";
        let formatted = format_ident(id, AdapterType::Fabric);
        assert_eq!(formatted, "[select]");
    }

    #[test]
    fn test_format_ident_quoted_default() {
        let id = "select";
        // Should panic because Postgres reserved is unimplemented
        let result = std::panic::catch_unwind(|| format_ident(id, AdapterType::Postgres));
        assert!(result.is_err());
    }

    #[test]
    fn test_need_quotes_empty() {
        assert!(need_quotes("", AdapterType::Fabric));
    }

    #[test]
    fn test_need_quotes_invalid_start_char() {
        assert!(need_quotes("1abc", AdapterType::Fabric));
        assert!(!need_quotes("_abc", AdapterType::Fabric));
        assert!(!need_quotes("@abc", AdapterType::Fabric));
        assert!(!need_quotes("#abc", AdapterType::Fabric));
        assert!(need_quotes("&abc", AdapterType::Fabric));
    }

    #[test]
    fn test_need_quotes_reserved_keyword() {
        assert!(need_quotes("SELECT", AdapterType::Fabric));
        assert!(need_quotes("select", AdapterType::Fabric));
    }

    #[test]
    fn test_is_valid_identifier_char() {
        assert!(is_valid_identifier_char(AdapterType::Fabric, 'a'));
        assert!(is_valid_identifier_char(AdapterType::Fabric, 'Z'));
        assert!(is_valid_identifier_char(AdapterType::Fabric, '_'));
        assert!(!is_valid_identifier_char(AdapterType::Fabric, '-'));
    }
}
