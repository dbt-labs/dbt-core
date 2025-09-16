use dbt_xdbc::Backend;

/// Returns true if the identifier SHOULD be quoted when formatting to
/// source code to preserve the casing of the provided identifier in this
/// backend's dialect.
pub fn need_quotes(backend: Backend, id: &str) -> bool {
    dbt_xdbc::sql::ident::must_be_quoted(id, backend)
        // In Snowflake, unquoted identifiers are normalized to uppercase,
        // therefore if the identifier contains any lowercase characters, it
        // needs to be quoted to preserve the original casing.
        || (matches!(backend, Backend::Snowflake) && id.chars().any(|c| c.is_ascii_lowercase()))
        // In Redshift, unquoted identifiers are normalized to lowercase,
        // therefore if the identifier contains any uppercase characters, it
        // needs to be quoted to preserve the original casing.
        || (matches!(backend, Backend::Redshift | Backend::RedshiftODBC) && id.chars().any(|c| c.is_ascii_uppercase()))
}
