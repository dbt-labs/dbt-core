use dbt_xdbc::Backend;

use crate::reserved_keywords::is_keyword_ignore_ascii_case;

/// The character used to quote identifiers in this backend's dialect.
pub const fn quote_char(backend: Backend) -> char {
    use Backend::*;
    match backend {
        BigQuery | Databricks | DatabricksODBC => '`',
        Snowflake => '"',
        Redshift | RedshiftODBC | Postgres => '"',
        Generic { .. } => '"',
    }
}

/// Returns true if the given character is a valid character for an
/// unquoted identifier in this backend's dialect.
pub fn is_valid_identifier_char(backend: Backend, c: char) -> bool {
    use Backend::*;
    match backend {
        BigQuery => c.is_alphanumeric() || ['_', '-', '$'].contains(&c),
        Snowflake => {
            // TODO: revert this once
            // https://github.com/sdf-labs/sdf/issues/3328 is fixed:
            // c.is_alphanumeric() || ['_', '`', '@'].contains(&c)
            c != '.' && c != quote_char(backend) && !c.is_whitespace() && c != '/' && c != ';'
        }
        // XXX: check these fallbacks against documentation of these dialects
        Postgres | Databricks | DatabricksODBC | Redshift | RedshiftODBC | Generic { .. } => {
            c.is_alphanumeric() || c == '_'
        }
    }
}

/// Returns true if the identifier has to be quoted when formatting to
/// source code form in this backend's dialect.
pub fn need_quotes(backend: Backend, id: &str) -> bool {
    // Empty identifiers have to be quoted
    id.is_empty()
        // If the first character is not alphabetic, the identifier has to be quoted
        || !id.chars().next().is_some_and( |c| c=='_' || c.is_ascii_alphabetic())
        // Invalid characters has to be quoted
        || id.chars().any(|c| !is_valid_identifier_char(backend, c)
            // BigQuery allows hyphens in unquoted identifiers in certain
            // contexts (e.g. table names), but we still quote them here
            || (matches!(backend, Backend::BigQuery) && c == '-'))
        // Reserved keywords has to be quoted
        || is_keyword_ignore_ascii_case(backend, id).is_some()
        // In Snowflake, unquoted identifiers are normalized to uppercase,
        // therefore if the identifier contains any lowercase characters, it
        // needs to be quoted to preserve the original casing.
        || (matches!(backend, Backend::Snowflake) && id.chars().any(|c| c.is_ascii_lowercase()))
        // In Redshift, unquoted identifiers are normalized to lowercase,
        // therefore if the identifier contains any uppercase characters, it
        // needs to be quoted to preserve the original casing.
        || (matches!(backend, Backend::Redshift | Backend::RedshiftODBC) && id.chars().any(|c| c.is_ascii_uppercase()))
}
