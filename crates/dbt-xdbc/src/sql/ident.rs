use core::fmt;

use crate::Backend;

use super::is_keyword_ignore_ascii_case;
use super::tokenizer::QuotingStyle;

/// An identifier and its provenance (from quoted or an plain identifier in the input source).
///
/// The name [Ident::Unquoted] means that the identifier was *quoted* in the
/// input source, but we call it "unquoted" because we have removed the
/// quoting characters and unescaped any escaped characters in the token.
///
/// This makes manipulating identifiers easier, because we don't have to
/// worry about escaping and quoting until we want to format the identifier
/// using the [Ident::display] method that takes a [Backend] and quotes the
/// identifier if necessary according to the specific dialect rules.
///
/// This also lets us preserve intentional quoting in the input source, even
/// if the identifier could have been represented as a plain identifier. For
/// example, the identifier `"MyTable"` (with quotes) will be represented
/// as `Unquoted('"', "MyTable")`, even though it could have been represented
/// as `Plain("MyTable")`. This is important for dialects like Snowflake
/// where unquoted identifiers are normalized to uppercase, so the quotes
/// are necessary to preserve the original casing.
#[derive(Debug, Clone)]
pub enum Ident {
    /// Identifier that was not quoted in the input source.
    Plain(String),
    /// Identifier that was quoted in the input source and has been unescaped.
    Unquoted(QuotingStyle, String),
}

impl AsRef<str> for Ident {
    fn as_ref(&self) -> &str {
        match self {
            Ident::Unquoted(_, s) => s.as_ref(),
            Ident::Plain(s) => s.as_ref(),
        }
    }
}

impl Ident {
    pub fn new(s: impl Into<String>, backend: Backend) -> Self {
        let s: String = s.into();
        if must_be_quoted(&s, backend) {
            Ident::Unquoted(canonical_quote(backend), s)
        } else {
            Ident::Plain(s)
        }
    }

    pub fn plain(s: impl Into<String>) -> Self {
        Ident::Plain(s.into())
    }

    pub fn unquoted(quote: QuotingStyle, s: impl Into<String>) -> Self {
        Ident::Unquoted(quote, s.into())
    }

    pub fn display(&self, backend: Backend) -> IdentDisplay<'_> {
        IdentDisplay(self, backend)
    }
}

pub struct IdentDisplay<'a>(&'a Ident, Backend);

impl fmt::Display for IdentDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Ident::Unquoted(quote, s) => _render_ident_in_quotes(*quote, s, self.1, f),
            Ident::Plain(s) => write!(f, "{s}"),
        }
    }
}

fn _render_ident_in_quotes(
    quote: QuotingStyle,
    s: &str,
    _backend: Backend,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    // TODO: use backend to determine how to escape the quote character
    write!(f, "{}", quote.opening())?;
    let closing: &str = quote.closing();
    let q = closing.chars().next().unwrap(); // `, ", or '
    for c in s.chars() {
        if c == q {
            // Escape the quote character by doubling it
            write!(f, "{closing}{closing}")?;
        } else {
            write!(f, "{c}")?;
        }
    }
    write!(f, "{closing}")
}

// TODO: implement a separate struct Idents that can be used as (BTree|Hash)(Map|Set) keys
// (a separate struct that binds the backend is needed because comparing Idents is backend-dependent)
//
// ## PostgreSQL
//
// The identifiers FOO, foo, and "foo" are considered the same by PostgreSQL, but "Foo" and "FOO"
// are different from these three and each other.
//
// https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS

/// The character used to quote identifiers in this backend's dialect.
pub const fn quote_char(backend: Backend) -> char {
    use Backend::*;
    match backend {
        BigQuery | Databricks | DatabricksODBC => '`',
        Snowflake => '"',
        Redshift | RedshiftODBC | Postgres | Salesforce => '"',
        Generic { .. } => '"',
    }
}

/// The canonical quoting style used to quote identifiers in this backend's dialect.
pub const fn canonical_quote(backend: Backend) -> QuotingStyle {
    use Backend::*;
    match backend {
        BigQuery | Databricks | DatabricksODBC => QuotingStyle::Backtick,
        Snowflake | Redshift | RedshiftODBC | Postgres | Salesforce => QuotingStyle::Double,
        Generic { .. } => QuotingStyle::Double,
    }
}

/// Returns true if the given character is a valid character for an
/// unquoted identifier in this backend's dialect.
pub fn is_valid_ident_char(c: char, backend: Backend) -> bool {
    use Backend::*;
    match backend {
        BigQuery => c.is_alphanumeric() || ['_', '-', '$'].contains(&c),
        Snowflake => {
            // TODO: revert this once
            // https://github.com/sdf-labs/sdf/issues/3328 is fixed:
            // c.is_alphanumeric() || ['_', '`', '@'].contains(&c)
            c != '.' && c != quote_char(backend) && !c.is_whitespace() && c != '/' && c != ';'
        }
        // TODO: check these fallbacks against documentation of these dialects
        Postgres
        | Databricks
        | DatabricksODBC
        | Redshift
        | RedshiftODBC
        | Salesforce
        | Generic { .. } => c.is_alphanumeric() || c == '_',
    }
}

/// Returns true iff the identifier absolutely MUST be quoted when formatting
/// to source code form in this backend's dialect.
///
/// For instance, if an column name contains a `-` character, it must be quoted,
/// because `-` is not a valid character in an unquoted identifier. It would be
/// a syntax error to write:
///
///     SELECT my-column FROM tbl;
///
/// But in PostgreSQL, the query above is valid if the identifier is quoted:
///
///    SELECT "my-column" FROM tbl;
///
/// IMPORTANT: an identifier can't represent user intention to quote or not quote.
/// If the user wrote `"MyTable"` (with quotes) in the input source, we should carry
/// that intention in a `Indent::Unquoted(Double, "MyTable")` value, even though
/// `must_be_quoted("MyTable", _)` returns false for all backends.
pub fn must_be_quoted(id: &str, backend: Backend) -> bool {
    let mut chars = id.chars();
    let first = match chars.next() {
        Some(c) => c,
        None => return true, // Empty identifiers MUST be quoted
    };

    // If the first character is not [_a-zA-Z], the identifier MUST be quoted
    if first != '_' && !first.is_ascii_alphabetic() {
        return true;
    }

    // Look for invalid characters on the rest of the identifier.
    // The first character is already checked above.
    let has_invalid_char = chars.any(|c| {
        !is_valid_ident_char(c, backend)
            // BigQuery allows hyphens in unquoted identifiers in certain
            // contexts (e.g. table names), but we still quote them here
            || (matches!(backend, Backend::BigQuery) && c == '-')
    });
    // Invalid characters MUST be in a quoted identifier (sometimes escaped)
    if has_invalid_char {
        return true;
    }

    // Reserved keywords MUST to be quoted to avoid confusing the lexer/parser
    is_keyword_ignore_ascii_case(id, backend).is_some()
}
