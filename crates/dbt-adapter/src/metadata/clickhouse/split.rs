//! Statement splitting for ClickHouse SQL.
//!
//! None of the engine's lexers covers ClickHouse's lexis: string literals use
//! backslash escapes (`\'`, `\\`, `\xNN`) *and* the server accepts
//! PostgreSQL-style heredocs (`$tag$ … $tag$`, which `persist_docs` relies
//! on). This scanner knows exactly the constructs that can hide a `;`:
//! quoted literals and identifiers, comments and heredocs (rules per
//! ClickHouse's `Lexer.cpp`, checked against a 26.3 server).

/// Split `sql` on top-level `;` (the `StmtSplitter::split` contract): the
/// input's surrounding whitespace and the delimiters are removed, everything
/// else is kept verbatim (statements carry their own leading whitespace and
/// comments); a trailing whitespace-only segment is dropped.
pub(crate) fn split_statements(sql: &str) -> Vec<&str> {
    let sql = sql.trim();
    let mut statements = Vec::new();
    let mut start = 0;
    let mut scanner = Scanner::new(sql);
    while let Some((pos, byte)) = scanner.next_significant() {
        if byte == b';' {
            statements.push(&sql[start..pos]);
            start = pos + 1;
        }
    }
    let tail = &sql[start..];
    if !tail.trim().is_empty() {
        statements.push(tail);
    }
    statements
}

/// Whether `sql` holds no statement at all: only whitespace and comments.
pub(crate) fn is_empty_or_comment_only(sql: &str) -> bool {
    Scanner::new(sql).next_significant().is_none()
}

/// Walks `sql` and yields the bytes that sit outside comments, string literals,
/// quoted identifiers and heredocs (whitespace excluded).
struct Scanner<'a> {
    sql: &'a str,
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Scanner<'a> {
    fn new(sql: &'a str) -> Self {
        Self {
            sql,
            bytes: sql.as_bytes(),
            pos: 0,
        }
    }

    fn next_significant(&mut self) -> Option<(usize, u8)> {
        while self.pos < self.bytes.len() {
            let pos = self.pos;
            let byte = self.bytes[pos];
            match byte {
                b'\'' | b'"' | b'`' => self.skip_quoted(byte),
                b'-' if self.bytes.get(pos + 1) == Some(&b'-') => self.skip_line_comment(),
                // MySQL-style `# ` and shebang `#!` comments; a bare `#word` is not a comment.
                b'#' if matches!(self.bytes.get(pos + 1), Some(b' ' | b'!')) => {
                    self.skip_line_comment()
                }
                b'/' if self.bytes.get(pos + 1) == Some(&b'*') => self.skip_block_comment(),
                b'$' => {
                    if let Some(end) = self.heredoc_end(pos) {
                        self.pos = end;
                    } else {
                        self.pos += 1;
                        return Some((pos, byte));
                    }
                }
                _ if byte.is_ascii_whitespace() => self.pos += 1,
                _ => {
                    self.pos += 1;
                    return Some((pos, byte));
                }
            }
        }
        None
    }

    /// Quoted literal/identifier with backslash escapes and doubled quotes
    /// (`'it''s'`). An unterminated literal swallows the rest of the input,
    /// which keeps a broken statement whole instead of splitting it.
    fn skip_quoted(&mut self, quote: u8) {
        self.pos += 1;
        while self.pos < self.bytes.len() {
            match self.bytes[self.pos] {
                b'\\' => self.pos += 2,
                b if b == quote => {
                    self.pos += 1;
                    if self.bytes.get(self.pos) == Some(&quote) {
                        self.pos += 1;
                    } else {
                        return;
                    }
                }
                _ => self.pos += 1,
            }
        }
        self.pos = self.bytes.len();
    }

    fn skip_line_comment(&mut self) {
        while self.pos < self.bytes.len() && self.bytes[self.pos] != b'\n' {
            self.pos += 1;
        }
    }

    fn skip_block_comment(&mut self) {
        self.pos += 2;
        while self.pos < self.bytes.len() {
            if self.bytes[self.pos] == b'*' && self.bytes.get(self.pos + 1) == Some(&b'/') {
                self.pos += 2;
                return;
            }
            self.pos += 1;
        }
    }

    /// End (exclusive) of the heredoc opening at `start`, i.e. just past the
    /// closing `$tag$`; the tag is any run of word characters, empty included.
    /// None when `start` does not open a heredoc or it never closes (then the
    /// `$` is an ordinary character).
    fn heredoc_end(&self, start: usize) -> Option<usize> {
        let rest = &self.sql[start + 1..];
        let tag_len = rest
            .bytes()
            .take_while(|b| b.is_ascii_alphanumeric() || *b == b'_')
            .count();
        if rest.as_bytes().get(tag_len) != Some(&b'$') {
            return None;
        }
        let opener = &self.sql[start..start + tag_len + 2];
        let body_start = start + opener.len();
        self.sql[body_start..]
            .find(opener)
            .map(|offset| body_start + offset + opener.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_on_top_level_semicolons_only() {
        assert_eq!(
            split_statements("SELECT 1; SELECT 2; SELECT 3;"),
            vec!["SELECT 1", " SELECT 2", " SELECT 3"]
        );
        assert_eq!(split_statements(";;;"), vec!["", "", ""]);
        assert_eq!(split_statements(""), Vec::<&str>::new());
        assert_eq!(
            split_statements("select 'a;b'; select \"c;d\"; select `e;f`"),
            vec!["select 'a;b'", " select \"c;d\"", " select `e;f`"]
        );
    }

    #[test]
    fn trims_the_input_but_not_the_statements() {
        assert_eq!(split_statements("  select 1  "), vec!["select 1"]);
        assert_eq!(
            split_statements("\n  select 1;\n  select 2;\n"),
            vec!["select 1", "\n  select 2"]
        );
        assert_eq!(split_statements("select 1 ;  "), vec!["select 1 "]);
    }

    #[test]
    fn keeps_comments_as_statements() {
        assert_eq!(
            split_statements("select 1; /* end comment */"),
            vec!["select 1", " /* end comment */"]
        );
        assert_eq!(
            split_statements("select 1 -- trailing ; comment\n; select 2"),
            vec!["select 1 -- trailing ; comment\n", " select 2"]
        );
        assert_eq!(
            split_statements("select 1; /* comment with ; */; select 2"),
            vec!["select 1", " /* comment with ; */", " select 2"]
        );
        assert_eq!(
            split_statements(
                "#!/usr/bin/clickhouse-local ; x\nselect 1 # hash ; comment\n; select 2"
            ),
            vec![
                "#!/usr/bin/clickhouse-local ; x\nselect 1 # hash ; comment\n",
                " select 2"
            ]
        );
        assert_eq!(
            split_statements("select '#'; select 1 #notacomment; select 2"),
            vec!["select '#'", " select 1 #notacomment", " select 2"]
        );
    }

    #[test]
    fn clickhouse_backslash_escapes_and_doubled_quotes() {
        assert_eq!(
            split_statements(r#"select 'don\'t; drop'; select '\x3F', 'a\\'; select 'it''s; ok'"#),
            vec![
                r#"select 'don\'t; drop'"#,
                r#" select '\x3F', 'a\\'"#,
                " select 'it''s; ok'"
            ]
        );
    }

    #[test]
    fn heredocs_hide_delimiters() {
        let sql = "alter table t modify comment $dbt_comment_literal_block$desc \"q\";\n'''x''' -- not a comment\n/* nor this */ $lbl$ nested $lbl$ and $$ bare $$ end$dbt_comment_literal_block$; select 1";
        let statements = split_statements(sql);
        assert_eq!(statements.len(), 2, "{statements:?}");
        assert!(statements[0].ends_with("end$dbt_comment_literal_block$"));
        assert_eq!(statements[1], " select 1");
        assert_eq!(
            split_statements("select $1x$a;b$1x$; select 2"),
            vec!["select $1x$a;b$1x$", " select 2"]
        );
        assert_eq!(
            split_statements("select $a b$x;y$a b$"),
            vec!["select $a b$x", "y$a b$"]
        );
        assert_eq!(
            split_statements("select $1; select 2"),
            vec!["select $1", " select 2"]
        );
        assert_eq!(
            split_statements("select $unterminated$ 1; select 2"),
            vec!["select $unterminated$ 1", " select 2"]
        );
    }

    #[test]
    fn unterminated_literal_keeps_the_rest_whole() {
        assert_eq!(
            split_statements("select 1; select 'unterminated; select 2"),
            vec!["select 1", " select 'unterminated; select 2"]
        );
    }

    #[test]
    fn empty_or_comment_only() {
        for sql in [
            "",
            "   ",
            "/* comment */",
            "-- line comment",
            "# hash comment",
            "#!/usr/bin/clickhouse-local",
            "  /* comment */  ",
            "/* comment */ -- line comment",
            "/* multi\nline\ncomment */",
        ] {
            assert!(is_empty_or_comment_only(sql), "{sql:?}");
        }
        for sql in [
            "select 1",
            "select /* comment */ 1",
            "-- comment\nselect 1",
            "/* comment */\nselect 1\n-- trailing",
            r#"select 'don\'t'"#,
            "#notacomment",
            ";",
        ] {
            assert!(!is_empty_or_comment_only(sql), "{sql:?}");
        }
    }
}
