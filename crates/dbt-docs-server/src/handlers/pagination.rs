//! Cursor pagination helpers shared by every list handler.
//!
//! Cursors are opaque base64 strings encoding `(sort_value, unique_id)` tuples.
//! Handlers run `LIMIT first + 1` to peek-detect `has_next_page`, then use the
//! trailing tuple to construct the next cursor.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

use crate::handlers::sql::escape_str;

/// Default page size when `?first` is omitted.
pub const DEFAULT_PAGE_SIZE: u32 = 100;
/// Maximum page size accepted; larger values are clamped.
pub const MAX_PAGE_SIZE: u32 = 1000;

/// Decoded cursor payload. Opaque to clients; the server can change the
/// internal shape at any time as long as the encode/decode stays in sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cursor {
    /// Sort column value at the boundary row, encoded as a string. `None`
    /// when the boundary row's sort value is NULL.
    pub sort_value: Option<String>,
    /// Unique id of the boundary row — tie-breaker so the cursor totally
    /// orders even when the primary sort column has duplicates.
    pub unique_id: String,
}

impl Cursor {
    /// Serialize to opaque base64.
    pub fn encode(&self) -> String {
        let json = serde_json::to_vec(self).expect("Cursor always serializes");
        URL_SAFE_NO_PAD.encode(json)
    }

    /// Decode from opaque base64. Returns a `&'static str` so the handler can
    /// pass it directly to `bad_request`.
    pub fn decode(s: &str) -> Result<Self, &'static str> {
        let bytes = URL_SAFE_NO_PAD.decode(s).map_err(|_| "invalid cursor")?;
        serde_json::from_slice(&bytes).map_err(|_| "invalid cursor")
    }
}

/// Sort direction.
#[derive(Copy, Clone, Debug)]
pub enum SortDir {
    Asc,
    Desc,
}

impl SortDir {
    pub fn as_sql(self) -> &'static str {
        match self {
            SortDir::Asc => "ASC",
            SortDir::Desc => "DESC",
        }
    }
}

/// `page_info` block returned on every LIST endpoint per ADR-6.
#[derive(Serialize)]
pub struct PageInfo {
    pub total_count: u64,
    pub start_cursor: Option<String>,
    pub end_cursor: Option<String>,
    pub has_next_page: bool,
}

/// Build the SQL `WHERE` fragment for advancing past a cursor.
///
/// `sort_expr` is the column expression used in `ORDER BY` (e.g. `n.name` or a
/// `CAST(...)` form — must be usable in `WHERE`, not a SELECT alias). `uid_expr`
/// is typically `n.unique_id`.
///
/// Generates an `OR`-of-conjunctions predicate that handles `NULLS LAST` for
/// both ASC and DESC:
///
/// - **Non-NULL cursor sort value, ASC**: advance via
///   `(sort > cv) OR (sort = cv AND uid > cuid) OR (sort IS NULL)`.
/// - **Non-NULL cursor sort value, DESC**: advance via
///   `(sort < cv) OR (sort = cv AND uid > cuid) OR (sort IS NULL)`.
/// - **NULL cursor sort value** (the cursor is in the NULL bucket — last by
///   NULLS LAST under either direction): only `(sort IS NULL AND uid > cuid)`.
pub fn cursor_where_fragment(
    sort_expr: &str,
    uid_expr: &str,
    dir: SortDir,
    cursor_sort_value: Option<&str>,
    cursor_unique_id: &str,
) -> String {
    let cuid_lit = format!("'{}'", escape_str(cursor_unique_id));
    match cursor_sort_value {
        Some(cv) => {
            let cv_lit = format!("'{}'", escape_str(cv));
            let cmp = match dir {
                SortDir::Asc => '>',
                SortDir::Desc => '<',
            };
            format!(
                "({sort_expr} {cmp} {cv_lit} OR ({sort_expr} = {cv_lit} AND {uid_expr} > {cuid_lit}) OR {sort_expr} IS NULL)"
            )
        }
        None => format!("({sort_expr} IS NULL AND {uid_expr} > {cuid_lit})"),
    }
}

/// Clamp a client-provided `first` value to the [1, MAX_PAGE_SIZE] range.
pub fn clamp_first(first: Option<u32>) -> u32 {
    first.unwrap_or(DEFAULT_PAGE_SIZE).clamp(1, MAX_PAGE_SIZE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_roundtrip_preserves_values() {
        let c = Cursor {
            sort_value: Some("zebra".into()),
            unique_id: "model.pkg.zebra".into(),
        };
        let encoded = c.encode();
        let decoded = Cursor::decode(&encoded).unwrap();
        assert_eq!(decoded.sort_value.as_deref(), Some("zebra"));
        assert_eq!(decoded.unique_id, "model.pkg.zebra");
    }

    #[test]
    fn cursor_roundtrip_null_sort_value() {
        let c = Cursor {
            sort_value: None,
            unique_id: "model.pkg.alpha".into(),
        };
        let encoded = c.encode();
        let decoded = Cursor::decode(&encoded).unwrap();
        assert!(decoded.sort_value.is_none());
        assert_eq!(decoded.unique_id, "model.pkg.alpha");
    }

    #[test]
    fn cursor_decode_garbage_returns_error() {
        assert_eq!(
            Cursor::decode("not base64!!!").unwrap_err(),
            "invalid cursor"
        );
        assert_eq!(Cursor::decode("aGVsbG8").unwrap_err(), "invalid cursor"); // valid base64, not JSON
    }

    #[test]
    fn cursor_where_fragment_asc_non_null() {
        let frag = cursor_where_fragment(
            "n.name",
            "n.unique_id",
            SortDir::Asc,
            Some("zebra"),
            "model.pkg.zebra",
        );
        assert!(frag.contains("n.name > 'zebra'"));
        assert!(frag.contains("n.name = 'zebra' AND n.unique_id > 'model.pkg.zebra'"));
        assert!(frag.contains("n.name IS NULL"));
    }

    #[test]
    fn cursor_where_fragment_desc_non_null() {
        let frag = cursor_where_fragment(
            "n.name",
            "n.unique_id",
            SortDir::Desc,
            Some("alpha"),
            "model.pkg.alpha",
        );
        assert!(frag.contains("n.name < 'alpha'"));
        assert!(frag.contains("n.name = 'alpha' AND n.unique_id > 'model.pkg.alpha'"));
    }

    #[test]
    fn cursor_where_fragment_null_cursor_uses_null_bucket() {
        let frag = cursor_where_fragment(
            "lr.executed_at",
            "n.unique_id",
            SortDir::Asc,
            None,
            "model.pkg.orphan",
        );
        assert_eq!(
            frag,
            "(lr.executed_at IS NULL AND n.unique_id > 'model.pkg.orphan')"
        );
    }

    #[test]
    fn cursor_where_fragment_escapes_quotes_in_literals() {
        let frag = cursor_where_fragment(
            "n.name",
            "n.unique_id",
            SortDir::Asc,
            Some("o'brien"),
            "model.pkg.o'brien",
        );
        // escape_str doubles single-quotes.
        assert!(frag.contains("n.name > 'o''brien'"));
        assert!(frag.contains("n.unique_id > 'model.pkg.o''brien'"));
    }

    #[test]
    fn clamp_first_defaults_and_caps() {
        assert_eq!(clamp_first(None), DEFAULT_PAGE_SIZE);
        assert_eq!(clamp_first(Some(0)), 1);
        assert_eq!(clamp_first(Some(50)), 50);
        assert_eq!(clamp_first(Some(10_000)), MAX_PAGE_SIZE);
    }
}
