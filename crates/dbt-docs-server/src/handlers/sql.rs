//! Small helpers for safely composing inline-string SQL from request inputs.
//!
//! Phase 2a uses string-interpolated SQL. The DuckDB instance is in-memory
//! and read-only (only views over parquet are registered) so the worst case
//! of an injected query is reading other already-readable views — but we
//! still sanitize to keep error surfaces small and behavior predictable.

/// Escape single quotes for SQL string literals.
pub fn escape_str(s: &str) -> String {
    s.replace('\'', "''")
}

/// Escape a string for use in a DuckDB ILIKE pattern.
///
/// Escapes `'` first (SQL string safety), then `\` (ILIKE escape char), then `%` and `_`.
/// The resulting string is safe to inline into a pattern like:
///   `column ILIKE '%' || '<escaped>' || '%' ESCAPE '\'`
///
/// **String literal disambiguation:**
/// - Rust source: `"ESCAPE '\\'"`  (two chars in Rust source: backslash, backslash)
/// - String value: `ESCAPE '\'`    (one backslash — the SQL escape character)
/// - Do NOT use raw strings (`r"..."`) for the ESCAPE clause; `r"ESCAPE '\\'"` would
///   produce the SQL `ESCAPE '\\'` which sets the escape character to TWO backslashes,
///   making all `\%` and `\_` patterns silently non-functional.
pub fn escape_ilike(s: &str) -> String {
    s.replace('\'', "''") // SQL string safety — must come before backslash escaping
        .replace('\\', r"\\") // ILIKE escape char — must come before % and _
        .replace('%', r"\%")
        .replace('_', r"\_")
}

/// True if the string contains only ASCII letters, digits, underscore, and
/// dot. Suitable for validating identifiers and dotted package names that
/// will be inlined into SQL.
pub fn is_safe_ident(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
}
