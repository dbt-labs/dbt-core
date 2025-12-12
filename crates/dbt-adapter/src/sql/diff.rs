use dbt_common::{AdapterError, AdapterErrorKind, AdapterResult};

use super::tokenizer::{AbstractToken, Token, abstract_tokenize, tokenize};
use regex::Regex;

pub fn compare_sql(actual: &str, expected: &str) -> AdapterResult<()> {
    // Canonicalize ignorable differences first
    let actual = canonicalize_query_tag(actual);
    let expected = canonicalize_query_tag(expected);
    let actual = canonicalize_uuid_literals(&actual);
    let expected = canonicalize_uuid_literals(&expected);
    let actual = canonicalize_elementary_tmp_suffix(&actual);
    let expected = canonicalize_elementary_tmp_suffix(&expected);

    // Heuristic: treat queries as equal if they only differ by a top-level
    // "select * from ( ... )" wrapper and benign CTE boundary syntax.
    if are_equivalent_ignoring_select_wrapper(&actual, &expected) {
        return Ok(());
    }

    // Create normalized SQL strings (remove all whitespace)
    let actual_normalized = actual
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>();
    let expected_normalized = expected
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>();
    // In addition, remove trailing comments /* ... */ in expected
    let expected_normalized = if expected_normalized.ends_with("*/") {
        let last_comment_start = expected_normalized.rfind("/*").unwrap();
        expected_normalized[..last_comment_start].to_string()
    } else {
        expected_normalized
    };

    // Direct comparison first
    if actual_normalized == expected_normalized {
        return Ok(());
    }

    // fuzzy comparison
    if fuzzy_compare_sql(&actual, &expected) {
        return Ok(());
    }

    // lightweight structural comparison
    if compare_sql_structurally(&actual, &expected) {
        return Ok(());
    }

    // SQL differs, generate visual diff information
    let diff_info = generate_visual_sql_diff(&actual, &expected);

    Err(AdapterError::new(
        AdapterErrorKind::UnexpectedResult,
        format!("SQL mismatch detected:\n\n{diff_info}"),
    ))
}

/// Lightweight structural comparator for SQL to relax overly strict mismatches.
/// Rules:
/// - Normalize whitespace significance by skipping it during parsing (inputs themselves are not mutated).
/// - If both look like: `select * from (<subquery>) <rest>` then recursively compare both `<subquery>` and `<rest>`.
/// - Else, if both look like: `with n1 as (<sub1>), ..., nk as (<subk>) <sub>` then
///   ensure corresponding names match and recursively compare each `<subi>`, then compare `<sub>`.
/// - Else, if both look like a `union all` chain at top level, split into components,
///   sort components, and recursively compare pair-wise.
/// - All recursive comparisons call back into `compare_sql`.
fn compare_sql_structurally(actual: &str, expected: &str) -> bool {
    // Quick trims to reduce edge whitespace noise
    let a = actual.trim();
    let b = expected.trim();
    if a.is_empty() && b.is_empty() {
        return true;
    }

    // 1) select * from (<subquery>) <rest>
    if let (Some((a_sub, a_rest)), Some((b_sub, b_rest))) = (
        parse_select_star_from_parenthesized(a),
        parse_select_star_from_parenthesized(b),
    ) {
        return compare_sql(a_sub, b_sub).is_ok() && compare_sql(a_rest, b_rest).is_ok();
    }

    // 2) with n1 as (<sub1>), ..., nk as (<subk>) <sub>
    if let (Some((a_ctes, a_tail)), Some((b_ctes, b_tail))) =
        (parse_with_clause(a), parse_with_clause(b))
    {
        if a_ctes.len() != b_ctes.len() {
            return false;
        }
        for ((a_name, a_sql), (b_name, b_sql)) in a_ctes.iter().zip(b_ctes.iter()) {
            // Compare CTE names for equality (case-sensitive as a conservative choice)
            if a_name != b_name {
                return false;
            }
            if compare_sql(a_sql, b_sql).is_err() {
                return false;
            }
        }
        return compare_sql(a_tail, b_tail).is_ok();
    }

    // 3) CREATE [OR REPLACE] <stuff> AS (<subquery>)
    if let (Some((a_stuff, a_sub)), Some((b_stuff, b_sub))) =
        (parse_create_as_subquery(a), parse_create_as_subquery(b))
    {
        return a_stuff == b_stuff && compare_sql(a_sub, b_sub).is_ok();
    }

    // 4) <sub1> union all <sub2> ... union all <sub_q>
    if let (Some(mut a_parts), Some(mut b_parts)) =
        (split_union_all_top_level(a), split_union_all_top_level(b))
    {
        if a_parts.len() > 1 && b_parts.len() > 1 && a_parts.len() == b_parts.len() {
            // Key-less lexicographic sort for deterministic pairing
            a_parts.sort();
            b_parts.sort();

            for (ax, bx) in a_parts.iter().zip(b_parts.iter()) {
                if compare_sql(ax, bx).is_err() {
                    return false;
                }
            }
            return true;
        }
    }

    false
}

fn skip_ws(s: &str, mut i: usize) -> usize {
    let bytes = s.as_bytes();
    while i < bytes.len() && (bytes[i] as char).is_whitespace() {
        i += 1;
    }
    i
}

fn starts_with_ci(s: &str, i: usize, kw: &str) -> bool {
    s[i..]
        .to_ascii_lowercase()
        .starts_with(&kw.to_ascii_lowercase())
}

fn eat_keyword_ci(s: &str, mut i: usize, kw: &str) -> Option<usize> {
    if starts_with_ci(s, i, kw) {
        i += kw.len();
        Some(i)
    } else {
        None
    }
}

fn find_matching_paren(s: &str, open_idx: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (idx, ch) in s.char_indices().skip(open_idx) {
        if ch == '(' {
            depth += 1;
        } else if ch == ')' {
            depth = depth.saturating_sub(1);
            if depth == 0 {
                return Some(idx);
            }
        }
    }
    None
}

fn parse_select_star_from_parenthesized(s: &str) -> Option<(&str, &str)> {
    // Recognize: select * from ( <subquery> ) <rest>
    let mut i = skip_ws(s, 0);
    i = eat_keyword_ci(s, i, "select")?;
    i = skip_ws(s, i);
    // Expect '*'
    let b = s.as_bytes();
    if i >= b.len() || b[i] as char != '*' {
        return None;
    }
    i += 1;
    i = skip_ws(s, i);
    i = eat_keyword_ci(s, i, "from")?;
    i = skip_ws(s, i);
    // Expect '('
    if i >= b.len() || b[i] as char != '(' {
        return None;
    }
    let open = i;
    let close = find_matching_paren(s, open)?;
    let sub = &s[open + 1..close];
    let rest = s[close + 1..].trim();
    Some((sub, rest))
}

fn parse_with_clause(s: &str) -> Option<(Vec<(String, String)>, &str)> {
    // Recognize: with n1 as (<sub1>), ..., nk as (<subk>) <tail>
    let mut i = skip_ws(s, 0);
    i = eat_keyword_ci(s, i, "with")?;
    let mut ctes: Vec<(String, String)> = Vec::new();
    let bytes = s.as_bytes();
    loop {
        i = skip_ws(s, i);
        // Parse CTE name up to 'as' (case-insensitive) that precedes '('
        let name_start = i;
        // Find 'as' while ensuring the following non-ws is '('
        let mut as_pos: Option<usize> = None;
        let mut j = i;
        while j < bytes.len() {
            // stop if we hit a top-level '(' before finding 'as' -> invalid for name
            if bytes[j] as char == '(' {
                break;
            }
            // try to match 'as'
            if starts_with_ci(s, j, "as") {
                // consume 'as' and any whitespace, then require '('
                let mut k = j + 2;
                k = skip_ws(s, k);
                if k < bytes.len() && bytes[k] as char == '(' {
                    as_pos = Some(j);
                    break;
                }
            }
            j += 1;
        }
        let as_pos = as_pos?;
        let name = s[name_start..as_pos].trim();
        if name.is_empty() {
            return None;
        }
        // Move to '('
        i = as_pos + 2;
        i = skip_ws(s, i);
        if i >= bytes.len() || bytes[i] as char != '(' {
            return None;
        }
        let open = i;
        let close = find_matching_paren(s, open)?;
        let sub = s[open + 1..close].trim().to_string();
        ctes.push((name.to_string(), sub));
        i = close + 1;
        i = skip_ws(s, i);
        if i < bytes.len() && bytes[i] as char == ',' {
            i += 1; // continue parsing next CTE
            continue;
        } else {
            // End of CTE list; the rest is the tail query
            let tail = s[i..].trim();
            return Some((ctes, tail));
        }
    }
}

fn split_union_all_top_level(s: &str) -> Option<Vec<&str>> {
    // Split on top-level "union all" (case-insensitive)
    let mut parts: Vec<&str> = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    let lower = s.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let ch = bytes[i] as char;
        if ch == '(' {
            depth += 1;
            i += 1;
            continue;
        } else if ch == ')' {
            depth = depth.saturating_sub(1);
            i += 1;
            continue;
        }
        if depth == 0 && lower[i..].starts_with("union") {
            // ensure it is "union all"
            let k = i + "union".len();
            // require at least one whitespace
            let k_after_ws = skip_ws(&lower, k);
            if k_after_ws > k && lower[k_after_ws..].starts_with("all") {
                // boundary found
                let left = s[start..i].trim();
                parts.push(left);
                // advance past "union all"
                i = k_after_ws + "all".len();
                start = i;
                continue;
            }
        }
        i += 1;
    }
    // push final segment
    let last = s[start..].trim();
    if !parts.is_empty() {
        parts.push(last);
        return Some(parts);
    }
    // If there were no splits, return None
    None
}

fn parse_create_as_subquery(s: &str) -> Option<(&str, &str)> {
    // Recognize: CREATE [OR REPLACE] <stuff> AS (<subquery>)
    // Case-insensitive for keywords; preserve exact <stuff> for equality check
    let mut i = skip_ws(s, 0);
    i = eat_keyword_ci(s, i, "create")?;
    i = skip_ws(s, i);
    // Optional "or replace"
    if let Some(mut j) = eat_keyword_ci(s, i, "or") {
        j = skip_ws(s, j);
        if let Some(k) = eat_keyword_ci(s, j, "replace") {
            i = k;
        } // if "or" not followed by "replace", keep original i (treat as not present)
    }
    let stuff_start = i;
    // Find 'as' followed by '(' (case-insensitive), not inside parentheses
    let lower = s.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let mut depth = 0usize;
    let mut as_pos: Option<usize> = None;
    let mut j = i;
    while j < bytes.len() {
        let ch = bytes[j] as char;
        if ch == '(' {
            depth += 1;
            j += 1;
            continue;
        } else if ch == ')' {
            depth = depth.saturating_sub(1);
            j += 1;
            continue;
        }
        if depth == 0 && lower[j..].starts_with("as") {
            let mut k = j + 2;
            k = skip_ws(&lower, k);
            if k < bytes.len() && (lower.as_bytes()[k] as char) == '(' {
                as_pos = Some(j);
                break;
            }
        }
        j += 1;
    }
    let as_pos = as_pos?;
    let stuff = s[stuff_start..as_pos].trim();
    // Move to '('
    let mut k = as_pos + 2;
    k = skip_ws(s, k);
    if k >= s.len() || s.as_bytes()[k] as char != '(' {
        return None;
    }
    let open = k;
    let close = find_matching_paren(s, open)?;
    let sub = s[open + 1..close].trim();
    Some((stuff, sub))
}

/// Replace the payload of `ALTER SESSION SET QUERY_TAG = '...';` with a fixed placeholder,
/// so differences in the query tag JSON/body are ignored during comparison.
fn canonicalize_query_tag(sql: &str) -> String {
    // Match: ALTER SESSION SET QUERY_TAG = '...'
    // Flags: (?i) case-insensitive, (?s) allow '.' to match newlines (defensive)
    // We specifically capture a single-quoted literal to avoid over-matching.
    static RE: once_cell::sync::Lazy<Regex> = once_cell::sync::Lazy::new(|| {
        Regex::new(r"(?is)\balter\s+session\s+set\s+query_tag\s*=\s*'[^']*'").unwrap()
    });
    RE.replace_all(sql, "alter session set query_tag = '__TAG__'")
        .to_string()
}

/// Replace single-quoted UUID string literals with a fixed `'UUID'` placeholder.
/// Example: '8f439b7e-752f-460a-8d1a-f469231d169c' -> 'UUID'
/// This is a blunt instrument. Ideally, we should address the problem at the root:
/// A lot of these are from {{ invocation_id }}. The value of the original invocation_id
/// is available in manifest.json. We should consider using it in replay. TODO: Do this!
fn canonicalize_uuid_literals(sql: &str) -> String {
    // Case-insensitive UUID regex inside single quotes
    static UUID_RE: once_cell::sync::Lazy<Regex> = once_cell::sync::Lazy::new(|| {
        Regex::new(r"(?i)'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'").unwrap()
    });
    UUID_RE.replace_all(sql, "'UUID'").to_string()
}

/// Replace dynamic tmp suffixes produced by some packages (e.g., elementary) that append
/// utc.now()-like timestamps to temporary table names, such as:
///   dbt_sources__tmp_20251203160139043240  ->  dbt_sources__tmp_TIMESTAMP
fn canonicalize_elementary_tmp_suffix(sql: &str) -> String {
    // Case-insensitive; match "__tmp_" followed by a long digit run (timestamps/unique suffixes)
    // Scope it to a plausible leading year 2000-2100 to avoid over-matching.
    // Example matched: "__tmp_20251203160139043240"
    static RE: once_cell::sync::Lazy<Regex> = once_cell::sync::Lazy::new(|| {
        Regex::new(r"(?i)(__tmp_)(?:20[0-9]{2}|2100)\d{8,}").unwrap()
    });
    RE.replace_all(sql, "${1}TIMESTAMP").to_string()
}

/// Check whether two SQL strings are identical modulo a top-level
/// "select * from ( ... )" wrapper and CTE boundary differences.
fn are_equivalent_ignoring_select_wrapper(actual: &str, expected: &str) -> bool {
    let norm_actual = normalize_for_wrapper_diff(actual);
    let norm_expected = normalize_for_wrapper_diff(expected);
    if norm_actual == norm_expected {
        return false;
    }
    let cleaned_actual = canonicalize_cte_boundaries(remove_select_star_wrapper(&norm_actual));
    let cleaned_expected = canonicalize_cte_boundaries(remove_select_star_wrapper(&norm_expected));
    if cleaned_actual == cleaned_expected {
        return true;
    }
    // Ignore extra parentheses without an expensive diff
    remove_all_parens(&cleaned_actual) == remove_all_parens(&cleaned_expected)
}

fn normalize_for_wrapper_diff(sql: &str) -> String {
    // Remove line comments starting with -- to end of line
    let mut out = String::with_capacity(sql.len());
    for line in sql.lines() {
        if let Some(idx) = line.find("--") {
            out.push_str(&line[..idx]);
            out.push('\n');
        } else {
            out.push_str(line);
            out.push('\n');
        }
    }
    // Collapse all whitespace and lowercase
    // Precompiled regex for performance
    static WS_RE: once_cell::sync::Lazy<Regex> =
        once_cell::sync::Lazy::new(|| Regex::new(r"\s+").unwrap());
    WS_RE.replace_all(&out, "").to_lowercase()
}

fn remove_select_star_wrapper(norm_sql: &str) -> String {
    // norm_sql is already lowercased and whitespace-free.
    const PATTERN: &str = "select*from(";
    if let Some(idx) = norm_sql.find(PATTERN) {
        let mut candidate = String::with_capacity(norm_sql.len());
        candidate.push_str(&norm_sql[..idx]);
        candidate.push_str(&norm_sql[idx + PATTERN.len()..]);
        while candidate.ends_with(')') {
            candidate.pop();
        }
        candidate
    } else {
        norm_sql.to_string()
    }
}

fn canonicalize_cte_boundaries(norm_sql: String) -> String {
    norm_sql.replace(")with", "),")
}

fn remove_all_parens(s: &str) -> String {
    s.replace(['(', ')'], "")
}

fn fuzzy_compare_sql(actual: &str, expected: &str) -> bool {
    let actual_tokens = tokenize(actual);
    let expected_tokens = tokenize(expected);

    let actual_tokens_without_comments = eliminate_comments(actual_tokens);
    let expected_tokens_without_comments = eliminate_comments(expected_tokens);

    let actual_abstract_tokens = abstract_tokenize(actual_tokens_without_comments);
    let expected_abstract_tokens = abstract_tokenize(expected_tokens_without_comments);

    let mut actual_index = 0;
    let mut expected_index = 0;
    let mut actual_abstract_token = None;
    let mut expected_abstract_token = None;
    while actual_index < actual_abstract_tokens.len()
        && expected_index < expected_abstract_tokens.len()
    {
        if actual_abstract_token.is_none() {
            actual_abstract_token = actual_abstract_tokens.get(actual_index).cloned();
        }
        if expected_abstract_token.is_none() {
            expected_abstract_token = expected_abstract_tokens.get(expected_index).cloned();
        }

        match (
            actual_abstract_token.as_ref().unwrap(),
            expected_abstract_token.as_ref().unwrap(),
        ) {
            (AbstractToken::Token(actual_token), AbstractToken::Token(expected_token)) => {
                let actual_token_value = actual_token.value.clone();
                let expected_token_value = expected_token.value.clone();
                if actual_token_value == expected_token_value
                    || (actual_token_value.to_lowercase() == "with"
                        && expected_token_value.to_lowercase() == "with")
                {
                    actual_abstract_token = None;
                    expected_abstract_token = None;
                    actual_index += 1;
                    expected_index += 1;
                } else if actual_token_value.starts_with(&expected_token_value) {
                    actual_abstract_token = Some(AbstractToken::Token(Token {
                        value: actual_token_value[expected_token_value.len()..].to_string(),
                        maybe_hash: false,
                    }));
                    expected_abstract_token = None;
                    expected_index += 1;
                } else if expected_token_value.starts_with(&actual_token_value) {
                    expected_abstract_token = Some(AbstractToken::Token(Token {
                        value: expected_token_value[actual_token_value.len()..].to_string(),
                        maybe_hash: false,
                    }));
                    actual_abstract_token = None;
                    actual_index += 1;
                } else {
                    return false;
                }
            }
            (AbstractToken::Hash { prefix, hash }, AbstractToken::Token(expected_token)) => {
                // e.g.
                // not_null_int_incident_io__inci_a94c7199c374113430d951145e2f84e8"
                // vs
                // not_null_int_incident_io__incident_field_entries_listed_unique_id"

                // First find the first 30 characters in expected
                let mut expected_prefix = expected_token.value.clone();
                expected_index += 1;
                while expected_prefix.len() < 30 {
                    if let Some(AbstractToken::Token(expected_token)) =
                        expected_abstract_tokens.get(expected_index)
                    {
                        expected_prefix = expected_prefix + &expected_token.value;
                        expected_index += 1;
                    } else {
                        break;
                    }
                }
                if expected_prefix.starts_with(prefix)
                    || expected_prefix
                        .strip_prefix("dbt_utils_")
                        .map(|s| s.starts_with(prefix) || prefix.starts_with(s))
                        .unwrap_or(false)
                {
                } else {
                    return false;
                }
                // Second, continue consuming expected tokens until the md5 hash matches the hash
                while expected_index < expected_abstract_tokens.len() {
                    match expected_abstract_tokens.get(expected_index).unwrap() {
                        AbstractToken::Token(expected_token) => {
                            let mut matched = false;
                            for (i, c) in expected_token.value.chars().enumerate() {
                                expected_prefix.push(c);
                                let expected_prefix_md5 =
                                    format!("{:x}", md5::compute(&expected_prefix));

                                if expected_prefix_md5 == *hash {
                                    matched = true;
                                    let expected_left_over =
                                        expected_token.value[i + 1..].to_string();
                                    if expected_left_over.is_empty() {
                                        expected_abstract_token = None;
                                        expected_index += 1;
                                    } else {
                                        expected_abstract_token =
                                            Some(AbstractToken::Token(Token {
                                                value: expected_left_over,
                                                maybe_hash: false,
                                            }));
                                    }
                                    break;
                                }
                            }
                            if !matched {
                                expected_index += 1;
                            } else {
                                break;
                            }
                        }
                        _ => {
                            return false;
                        }
                    }
                }
                actual_abstract_token = None;
                actual_index += 1;
            }
            (AbstractToken::Token(_), AbstractToken::Hash { .. }) => {
                return false;
            }
            (
                AbstractToken::Hash {
                    hash: actual_hash, ..
                },
                AbstractToken::Hash {
                    hash: expected_hash,
                    ..
                },
            ) => {
                // e.g.
                // source_unique_combination_of_c_7d86b29e62ff0d9a2521eecdb583ae14
                // vs
                // dbt_utils_source_unique_combin_7d86b29e62ff0d9a2521eecdb583ae14
                if actual_hash != expected_hash {
                    return false;
                }
                actual_abstract_token = None;
                expected_abstract_token = None;
                actual_index += 1;
                expected_index += 1;
            }
            // we don't care about the timestamp value
            (AbstractToken::Timestamp { .. }, AbstractToken::Timestamp { .. }) => {
                actual_abstract_token = None;
                expected_abstract_token = None;
                actual_index += 1;
                expected_index += 1;
            }
            (AbstractToken::Timestamp { .. }, _) | (_, AbstractToken::Timestamp { .. }) => {
                return false;
            }
        }
    }

    if actual_index == actual_abstract_tokens.len()
        && expected_index == expected_abstract_tokens.len()
    {
        return true;
    }

    false
}

fn eliminate_comments(tokens: Vec<Token>) -> Vec<Token> {
    let mut result = Vec::new();
    let mut in_comment = false;
    for token in tokens {
        if token.matches("\n") {
            if in_comment {
                in_comment = false;
            }
        } else if token.value.starts_with("--") {
            in_comment = true;
            if token.value.starts_with("--EPHEMERAL-SELECT-WRAPPER") {
                result.push(token);
            }
        } else if !in_comment {
            result.push(token);
        }
    }
    result
}
fn generate_visual_sql_diff(actual: &str, expected: &str) -> String {
    let mut diff_output = String::new();
    diff_output.push_str("Visual SQL Diff (ignoring all whitespace):\n");
    diff_output.push_str("==========================================\n\n");

    // Create normalized strings
    let actual_normalized = actual
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>();
    let expected_normalized = expected
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>();

    // Compare normalized strings
    if actual_normalized == expected_normalized {
        diff_output.push_str("No differences found.\n");
        return diff_output;
    }

    // Show original SQL first
    diff_output.push_str("Original SQL:\n");
    diff_output.push_str("-------------\n");
    diff_output.push_str("Actual:\n");
    diff_output.push_str(&format!("{actual}\n\n"));
    diff_output.push_str("Expected:\n");
    diff_output.push_str(&format!("{expected}\n\n"));

    // Show normalized comparison with visual markers
    let diff_markers = create_normalized_diff_markers(&actual_normalized, &expected_normalized);

    diff_output.push_str("Normalized Comparison (whitespace removed):\n");
    diff_output.push_str("-------------------------------------------\n");
    diff_output.push_str(&format!("Actual  : {actual_normalized}\n"));
    diff_output.push_str(&format!("Expected: {expected_normalized}\n"));

    if !diff_markers.trim().is_empty() {
        diff_output.push_str(&format!("Diff    : {diff_markers}\n"));
    }

    diff_output
}

fn create_normalized_diff_markers(actual: &str, expected: &str) -> String {
    use similar::{ChangeTag, TextDiff};

    let diff = TextDiff::from_chars(actual, expected);
    let mut result = String::new();

    for change in diff.iter_all_changes() {
        match change.tag() {
            ChangeTag::Equal => {
                // Common parts - use spaces
                result.push_str(&" ".repeat(change.value().len()));
            }
            ChangeTag::Delete => {
                // Deleted from actual (missing in expected) - don't add markers for expected
                // since we're showing markers for the expected string
            }
            ChangeTag::Insert => {
                // Inserted in expected (extra in expected) - mark as different
                result.push_str(&"-".repeat(change.value().len()));
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_test_primitives::assert_contains;

    #[test]
    fn test_compare_sql_identical_ignore_whitespace() {
        let sql1 = "SELECT   *\nFROM    users";
        let sql2 = "SELECT*FROMusers";

        let result = compare_sql(sql1, sql2);
        assert!(
            result.is_ok(),
            "Should be OK when SQL is identical ignoring whitespace"
        );
    }

    #[test]
    fn test_compare_sql_case_sensitive() {
        let sql1 = "SELECT * FROM users";
        let sql2 = "select * from users";

        let result = compare_sql(sql1, sql2);
        assert!(result.is_err(), "Should fail when case differs");
    }

    #[test]
    fn test_compare_sql_different_content() {
        let sql1 = "SELECT * FROM users WHERE id = 1";
        let sql2 = "SELECT * FROM orders WHERE id = 2";

        let result = compare_sql(sql1, sql2);
        assert!(result.is_err(), "Should fail when SQL content differs");
    }

    #[test]
    fn test_compare_sql_length_difference() {
        let sql1 = "SELECT * FROM users";
        let sql2 = "SELECT * FROM users WHERE active = true";

        let result = compare_sql(sql1, sql2);
        assert!(result.is_err(), "Should fail when SQL length differs");
    }

    #[test]
    fn test_visual_diff_markers() {
        let sql1 = "SELECT id, name FROM users";
        let sql2 = "SELECT id, email FROM users";

        let diff = generate_visual_sql_diff(sql1, sql2);

        // Test that it shows both original and normalized versions
        assert!(diff.contains(sql1));
        assert!(diff.contains(sql2));
        assert_contains!(diff, "SELECTid,nameFROMusers");
        assert_contains!(diff, "SELECTid,emailFROMusers");
    }

    #[test]
    fn test_multiline_sql_ignores_newlines() {
        let sql1 = "SELECT\nu.id,\nu.name\nFROM users u";
        let sql2 = "SELECT u.id, u.name FROM users u";

        let result = compare_sql(sql1, sql2);
        assert!(
            result.is_ok(),
            "Should ignore newlines and whitespace differences"
        );
    }

    #[test]
    fn test_multiline_sql_detects_content_differences() {
        let sql1 = r#"SELECT
            u.id,
            u.name
        FROM users u"#;

        let sql2 = r#"SELECT
            u.id,
            u.email
        FROM users u"#;

        let result = compare_sql(sql1, sql2);
        assert!(
            result.is_err(),
            "Should detect content differences even with newlines"
        );
    }

    #[test]
    fn test_empty_sql_comparison() {
        let result1 = compare_sql("", "");
        assert!(result1.is_ok(), "Empty SQL should match empty SQL");

        let result2 = compare_sql("SELECT 1", "");
        assert!(result2.is_err(), "Non-empty SQL should not match empty SQL");

        let result3 = compare_sql("", "SELECT 1");
        assert!(result3.is_err(), "Empty SQL should not match non-empty SQL");
    }

    #[test]
    fn test_whitespace_only_sql() {
        let sql1 = "   \n\t  ";
        let sql2 = "  \t\n   ";

        let result = compare_sql(sql1, sql2);
        assert!(
            result.is_ok(),
            "Whitespace-only SQL should match regardless of type/order"
        );
    }

    #[test]
    fn test_smart_diff_markers_with_similar() {
        let actual = "SELECT name FROM users";
        let expected = "SELECT email FROM users";

        let actual_norm = actual
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect::<String>();
        let expected_norm = expected
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect::<String>();
        let markers = create_normalized_diff_markers(&actual_norm, &expected_norm);

        // Test that normalized strings are what we expect
        assert_eq!(actual_norm, "SELECTnameFROMusers");
        assert_eq!(expected_norm, "SELECTemailFROMusers");

        // Test that markers are generated (using similar crate's intelligent diff)
        assert!(!markers.is_empty(), "Should generate diff markers");

        // With similar crate, we expect:
        // - Common parts (SELECT, FROMusers) marked with spaces
        // - Different parts (name vs email) marked appropriately

        // The exact pattern depends on similar's algorithm, but we can test basic properties
        let marker_chars: Vec<char> = markers.chars().collect();
        let expected_chars: Vec<char> = expected_norm.chars().collect();

        assert_eq!(
            marker_chars.len(),
            expected_chars.len(),
            "Marker length should match expected string length"
        );

        // Check that we have both spaces (common parts) and other markers (different parts)
        let has_spaces = marker_chars.contains(&' ');
        let has_diff_markers = marker_chars.contains(&' ');

        assert!(has_spaces, "Should have spaces for common parts");
        assert!(
            has_diff_markers,
            "Should have difference markers for different parts"
        );
    }

    #[test]
    fn test_placeholder_replacement_differences() {
        let actual = "SELECT %s, %s FROM table";
        let expected = "SELECT 1, 'test' FROM table";

        let result = compare_sql(actual, expected);
        assert!(
            result.is_err(),
            "Should detect placeholder vs value differences"
        );

        let diff = generate_visual_sql_diff(actual, expected);
        assert_contains!(diff, "SELECT%s,%sFROMtable");
        assert_contains!(diff, "SELECT1,'test'FROMtable");
    }

    #[test]
    fn test_complex_whitespace_scenarios() {
        // Test various whitespace combinations
        let scenarios = vec![
            ("SELECT\t*\nFROM\r\ntable", "SELECT * FROM table"),
            ("  SELECT  *  FROM  table  ", "SELECT*FROMtable"),
            ("SELECT\n\n\n*\n\n\nFROM\n\n\ntable", "SELECT * FROM table"),
        ];

        for (sql1, sql2) in scenarios {
            let result = compare_sql(sql1, sql2);
            assert!(
                result.is_ok(),
                "Should ignore all whitespace variations: '{sql1}' vs '{sql2}'"
            );
        }
    }

    #[test]
    fn test_case_sensitivity_preserved() {
        // These should be different because case matters
        let test_cases = vec![
            ("SELECT", "select"),
            ("FROM", "from"),
            ("WHERE", "where"),
            ("users", "USERS"),
        ];

        for (upper, lower) in test_cases {
            let sql1 = format!("{upper} * FROM table");
            let sql2 = format!("{lower} * FROM table");

            let result = compare_sql(&sql1, &sql2);
            assert!(
                result.is_err(),
                "Should be case sensitive: '{upper}' vs '{lower}'"
            );
        }
    }

    #[test]
    fn test_empty_and_whitespace_edge_cases() {
        let test_cases = vec![
            ("", "", true),           // Both empty should match
            ("   ", "\t\n", true),    // All whitespace should match
            ("SELECT", "", false),    // Content vs empty should not match
            ("", "SELECT", false),    // Empty vs content should not match
            ("   ", "SELECT", false), // Whitespace vs content should not match
        ];

        for (sql1, sql2, should_match) in test_cases {
            let result = compare_sql(sql1, sql2);
            if should_match {
                assert!(result.is_ok(), "Should match: '{sql1}' vs '{sql2}'");
            } else {
                assert!(result.is_err(), "Should not match: '{sql1}' vs '{sql2}'");
            }
        }
    }

    #[test]
    fn test_with_clause_vs_simple_select() {
        let simple_select = "SELECT * FROM users";
        let with_clause_select = r#"WITH temp_table AS (
            SELECT id, name FROM customers
        )
        SELECT * FROM users"#;

        let result = compare_sql(simple_select, with_clause_select);
        assert!(
            result.is_err(),
            "Should detect difference between simple SELECT and WITH clause"
        );

        // Verify the diff shows the WITH clause difference
        let diff = generate_visual_sql_diff(simple_select, with_clause_select);
        assert_contains!(diff, "SELECT*FROMusers");
        assert_contains!(
            diff,
            "WITHtemp_tableAS(SELECTid,nameFROMcustomers)SELECT*FROMusers"
        );
    }

    #[test]
    fn test_compare_sql_with_truncated_test_name() {
        let sql1 = r#"    alter session set query_tag = '{"dbt_environment_name": "default", "dbt_job_id": "not set", "dbt_run_id": "not set", "dbt_run_reason": "development_and_testing", "dbt_project_name": "fishtown_internal_analytics", "dbt_user_name": "ZHONG.XU", "dbt_model_name": "not_null_int_incident_io__inci_a94c7199c374113430d951145e2f84e8", "dbt_materialization_type": "test", "dbt_incremental_full_refresh": "false", "dbt_is_cold_storage_refresh": "false", "dbt_invocation_env": "null"}'"#;
        let sql2 = r#"    alter session set query_tag = '{"dbt_environment_name": "default", "dbt_job_id": "not set", "dbt_run_id": "not set", "dbt_run_reason": "development_and_testing", "dbt_project_name": "fishtown_internal_analytics", "dbt_user_name": "ZHONG.XU", "dbt_model_name": "not_null_int_incident_io__incident_field_entries_listed_unique_id", "dbt_materialization_type": "test", "dbt_incremental_full_refresh": "false", "dbt_is_cold_storage_refresh": "false", "dbt_invocation_env": "null"}'"#;

        let result = compare_sql(sql1, sql2);
        assert!(
            result.is_ok(),
            "Should ignore difference between truncated and full test name"
        );
    }

    #[test]
    fn test_compare_sql_with_dbt_utils_table_name() {
        let sql1 = r#"create or replace transient table analytics_dev.dbt_zhongxu.source_unique_combination_of_c_7d86b29e62ff0d9a2521eecdb583ae14
             as
            (
    with validation_errors as (
        select
            incident_id, incident_timestamp_id
        from raw.fivetran_incidentio.incident_timestamp_value
        group by incident_id, incident_timestamp_id
        having count(*) > 1
    )
    select *
    from validation_errors
            );"#;
        let sql2 = r#"create or replace transient table analytics_dev.dbt_zhongxu.dbt_utils_source_unique_combin_7d86b29e62ff0d9a2521eecdb583ae14
        as (
    with validation_errors as (
        select
            incident_id, incident_timestamp_id
        from raw.fivetran_incidentio.incident_timestamp_value
        group by incident_id, incident_timestamp_id
        having count(*) > 1
    )
    select *
    from validation_errors
        )
    ;
    "#;

        let result = compare_sql(sql1, sql2);
        assert!(result.is_ok(), "Should ignore difference for dbt_utils_");
    }

    #[test]
    fn test_compare_sql_with_dbt_utils_table_name_2() {
        let sql1 = r#"alter session set query_tag = '{"dbt_environment_name": "default", "dbt_job_id": "not set", "dbt_run_id": "not set", "dbt_run_reason": "development_and_testing", "dbt_project_name": "fishtown_internal_analytics", "dbt_user_name": "ZHONG.XU", "dbt_model_name": "source_unique_combination_of_c_7d86b29e62ff0d9a2521eecdb583ae14", "dbt_materialization_type": "test", "dbt_incremental_full_refresh": "false", "dbt_is_cold_storage_refresh": "false", "dbt_invocation_env": "null"}'"#;
        let sql2 = r#"alter session set query_tag = '{"dbt_environment_name": "default", "dbt_job_id": "not set", "dbt_run_id": "not set", "dbt_run_reason": "development_and_testing", "dbt_project_name": "fishtown_internal_analytics", "dbt_user_name": "ZHONG.XU", "dbt_model_name": "dbt_utils_source_unique_combination_of_columns_incident_io_incident_timestamp_value_incident_id__incident_timestamp_id", "dbt_materialization_type": "test", "dbt_incremental_full_refresh": "false", "dbt_is_cold_storage_refresh": "false", "dbt_invocation_env": "null"}'"#;
        let result = compare_sql(sql1, sql2);
        assert!(result.is_ok(), "Should ignore difference for dbt_utils_");
    }

    #[test]
    fn test_compare_sql_timestamp() {
        let sql1 = r#"delete from ANALYTICS.intermediate.int_serp_trends 
      where created_date >= '2025-09-10T18:07:45.449898-07:00'"#;
        let sql2 = r#"delete from ANALYTICS.intermediate.int_serp_trends 
      where created_date >= '2025-09-10T14:16:52.500487'"#;
        let result = compare_sql(sql1, sql2);
        assert!(
            result.is_ok(),
            "Should ignore difference for timestamp value difference"
        );
    }

    #[test]
    fn test_compare_sql_timestamp_ignore_t() {
        let sql1 = r#"delete from ANALYTICS.intermediate.int_serp_trends 
      where created_date >= '2025-09-10T18:07:45.449898'"#;
        let sql2 = r#"delete from ANALYTICS.intermediate.int_serp_trends 
      where created_date >= '2025-09-1014:16:52.500487'"#;
        let result = compare_sql(sql1, sql2);
        assert!(
            result.is_ok(),
            "Should ignore difference for timestamp value difference"
        );
    }

    #[test]
    fn test_compare_sql_timestamp_ignore_t2() {
        let sql1 = r#"delete from ANALYTICS.intermediate.int_serp_trends 
      where created_date >= '2025-09-10T18:07:45.449898'"#;
        let sql2 = r#"delete from ANALYTICS.intermediate.int_serp_trends 
      where created_date >= '2025-09-10 14:16:52.500487'"#;
        let result = compare_sql(sql1, sql2);
        assert!(
            result.is_ok(),
            "Should ignore difference for timestamp value difference"
        );
    }

    #[test]
    fn test_compare_ephemeral_model() {
        let sql1 = r#"
create or replace transient table x.y.z
    as (with u as (
with
v as (
    select 1
from w
),
select *
from unioned
)
--EPHEMERAL-SELECT-WRAPPER-START
select * from (
with base as (
    select *
    from u
)
select *
from aggregated
--EPHEMERAL-SELECT-WRAPPER-END
)
    )
;
"#;
        let sql2 = r#"
create or replace transient table x.y.z
    as (with u as (
with
v as (
    select 1
from w
),
select *
from unioned
)
, base as (
    select *
    from u
)
select *
from aggregated
    )
;"#;
        let result = compare_sql(sql1, sql2);
        assert!(result.is_ok(), "Should match");
    }

    #[test]
    fn test_comment_in_ephemeral_model() {
        let sql1 = r#"
create or replace  temporary view DB.SCHEMA.model_name__dbt_tmp
  
   as (
    with __dbt__cte__stg_source_a as (
SELECT
  *
FROM
  source_db.metadata.table_a
), __dbt__cte__stg_source_b as (
SELECT 
  *
FROM
  source_db.metadata.table_b
)
--EPHEMERAL-SELECT-WRAPPER-START
select * from (


-- Do not allow a full refresh of this model

  


-- This model contains aggregated statistics
-- Every day, the data is extracted and stored for analysis

WITH aggregated_data AS (
  SELECT 
    entity_id
    , COUNT(DISTINCT field_name) as field_count
  FROM 
    __dbt__cte__stg_source_a
  GROUP BY entity_id
)

SELECT
  t.schema_name
  , t.entity_name
  , t.num_rows
  , t.size_bytes
  , s.field_count
  , CURRENT_DATE() AS snapshot_date
FROM
  __dbt__cte__stg_source_b t
LEFT OUTER JOIN 
  aggregated_data s ON t.entity_id = s.entity_id
--EPHEMERAL-SELECT-WRAPPER-END
)
  );
"#;
        let sql2 = r#"
create or replace  temporary view DB.SCHEMA.model_name__dbt_tmp
  
  
  
  
  as (
    

-- Do not allow a full refresh of this model

  


-- This model contains aggregated statistics
-- Every day, the data is extracted and stored for analysis

WITH  __dbt__cte__stg_source_a as (
SELECT
  *
FROM
  source_db.metadata.table_a
),  __dbt__cte__stg_source_b as (
SELECT 
  *
FROM
  source_db.metadata.table_b
), aggregated_data AS (
  SELECT 
    entity_id
    , COUNT(DISTINCT field_name) as field_count
  FROM 
    __dbt__cte__stg_source_a
  GROUP BY entity_id
)

SELECT
  t.schema_name
  , t.entity_name
  , t.num_rows
  , t.size_bytes
  , s.field_count
  , CURRENT_DATE() AS snapshot_date
FROM
  __dbt__cte__stg_source_b t
LEFT OUTER JOIN 
  aggregated_data s ON t.entity_id = s.entity_id
  );
"#;
        let result = compare_sql(sql1, sql2);
        assert!(result.is_ok(), "Should match");
    }

    #[test]
    fn test_compare_sql_query_tag_payload_ignored() {
        let actual = r#"    alter session set query_tag = '{""model_name"":""stg_base_orders"",""env"":""PRD"",""job"":{""run_id"":"""",""execution_date"":"""",""start_date"":""""}}'"#;
        let expected = r#"    alter session set query_tag = '{""env"": ""PRD"", ""job"": {""execution_date"": """", ""run_id"": """", ""start_date"": """"}, ""model_name"": ""stg_base_orders""}'"#;
        let result = compare_sql(actual, expected);
        assert!(
            result.is_ok(),
            "Query tag payload differences should be ignored"
        );
    }

    #[test]
    fn test_compare_sql_uuid_literals_ignored() {
        let actual = r#"
INSERT INTO
    PROD_SSAP_AUDIT.ABAC.ABAC_JOB_RUN
    (
        system_run_id,
        job_id,
        batch_run_id,
        job_start_dttm,
        job_end_dttm,
        job_start_dttm_utc,
        job_end_dttm_utc,
        job_status,
        last_updt_dttm,
        last_updt_uid
    )
SELECT
    '019a71ca-e5ad-7ca3-99d8-49b58a470d82' AS system_run_id,
    962 AS job_id,
    47217 AS batch_run_id,
    CURRENT_TIMESTAMP() AS job_start_dttm,
    NULL AS job_end_dttm,
    CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS job_start_dttm_utc,
    NULL AS job_end_dttm_utc,
    'RUNNING' AS job_status,
    CURRENT_TIMESTAMP() AS last_updt_dttm,
    CURRENT_USER() AS last_updt_uid
FROM
    PROD_SSAP_AUDIT.ABAC.ABAC_JOB AS abac_job
WHERE
    abac_job.job_target = 'ldw_prtnr_all_wk_sumr_sales'
        "#;

        let expected = r#"
INSERT INTO
    PROD_SSAP_AUDIT.ABAC.ABAC_JOB_RUN
    (
        system_run_id,
        job_id,
        batch_run_id,
        job_start_dttm,
        job_end_dttm,
        job_start_dttm_utc,
        job_end_dttm_utc,
        job_status,
        last_updt_dttm,
        last_updt_uid
    )
SELECT
    '8f439b7e-752f-460a-8d1a-f469231d169c' AS system_run_id,
    962 AS job_id,
    47217 AS batch_run_id,
    CURRENT_TIMESTAMP() AS job_start_dttm,
    NULL AS job_end_dttm,
    CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS job_start_dttm_utc,
    NULL AS job_end_dttm_utc,
    'RUNNING' AS job_status,
    CURRENT_TIMESTAMP() AS last_updt_dttm,
    CURRENT_USER() AS last_updt_uid
FROM
    PROD_SSAP_AUDIT.ABAC.ABAC_JOB AS abac_job
WHERE
    abac_job.job_target = 'ldw_prtnr_all_wk_sumr_sales'
        "#;

        let result = compare_sql(actual, expected);
        assert!(result.is_ok(), "UUID literal differences should be ignored");
    }

    #[test]
    fn test_wrapper_diff_only_equivalence() {
        // Simple case: one side wraps the other in select * from ( ... )
        let with_cte = r#"
with base as (
    select 1 as id
)
select *
from base
"#;
        let wrapped = r#"
select * from (
with base as (
    select 1 as id
)
select *
from base
)
"#;
        let result = compare_sql(wrapped, with_cte);
        assert!(
            result.is_ok(),
            "Wrapper-only difference with identical body should be ignored"
        );
    }

    #[test]
    fn test_compare_sql_elementary_tmp_suffix_ignored() {
        let actual = r#"
create or replace temporary table abc_db.abc_production_models_elementary.dbt_sources__tmp_20251203160139043240
as (

    SELECT
        *
    FROM abc_db.abc_production_models_elementary.dbt_sources
    WHERE 1 = 0
)
;
"#;
        let expected = r#"
create or replace temporary table abc_db.abc_production_models_elementary.dbt_sources__tmp_20240102030405060708
as (

    SELECT
        *
    FROM abc_db.abc_production_models_elementary.dbt_sources
    WHERE 1 = 0
)
;
"#;
        let result = compare_sql(actual, expected);
        assert!(
            result.is_ok(),
            "Dynamic tmp suffixes starting with a plausible year should be ignored"
        );
    }

    #[test]
    fn test_structural_union_ordering_equivalence() {
        let actual = r#"select * from (
        



with filtered_information_schema_columns as (
    
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('aftership')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from iamcurious_db.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('iamcurious_production_staging')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('google_ads')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('google_analytics')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from iamcurious_db.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('iamcurious_production')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('klaviyo')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('macroeconomic_data')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('mailchimp')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from machine_learning.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('predictions')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('mongodb')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('postgres_rds')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from iamcurious_db.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('iamcurious_schema')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('resmagic_api')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('returnly')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('sendgrid')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('shopify')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('information_schema')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('stripe')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('zendesk')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('zucc_meta')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from iamcurious_db.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('iamcurious_production_models')

)
        
    


)

select *
from filtered_information_schema_columns
where full_table_name is not null
    ) as __dbt_sbq
    where false
    limit 0
        "#;

        let expected = r#"select * from (
        



with filtered_information_schema_columns as (
    
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('aftership')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from iamcurious_db.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('iamcurious_production_staging')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('google_ads')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('google_analytics')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from iamcurious_db.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('iamcurious_production')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('klaviyo')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('macroeconomic_data')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('mailchimp')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from machine_learning.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('predictions')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from iamcurious_db.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('iamcurious_schema')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('returnly')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('sendgrid')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('information_schema')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('shopify')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('stripe')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('zendesk')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('zucc_meta')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('mongodb')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('postgres_rds')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from raw.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('resmagic_api')

)
        
            union all
        
    
        (
    

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from iamcurious_db.INFORMATION_SCHEMA.COLUMNS
    where upper(table_schema) = upper('iamcurious_production_models')

)
        
    


)

select *
from filtered_information_schema_columns
where full_table_name is not null
    ) as __dbt_sbq
    where false
    limit 0
        "#;

        let result = compare_sql(actual, expected);
        assert!(
            result.is_ok(),
            "Should treat union-all sets equal regardless of order within the CTE body"
        );
    }
}
