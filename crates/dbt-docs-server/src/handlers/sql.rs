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

/// True if the string contains only ASCII letters, digits, underscore, and
/// dot. Suitable for validating identifiers and dotted package names that
/// will be inlined into SQL.
pub fn is_safe_ident(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
}
