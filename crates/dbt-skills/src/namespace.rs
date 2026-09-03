//! dbt's namespace inside a provider's skills directory.
//!
//! Every skill dbt installs lands in `dbt-<name>/` and declares
//! `name: dbt-<name>`, so a directory listing shows at a glance which skills
//! dbt manages. That convention is the whole ownership mechanism: no sidecar,
//! no recorded state, nothing to read back.
//!
//! Both the directory and the frontmatter `name` must carry the prefix. The
//! providers disagree about which one is a skill's identity — Claude Code uses
//! the directory and treats `name` as a display label, while Codex and Gemini
//! CLI use `name` and fall back to the directory — so changing only one would
//! leave the namespace absent on half of them.

use dbt_common::{ErrorCode, FsResult, fs_err};

/// Marks a skill directory, and its declared `name`, as dbt-installed.
///
/// Lowercase and hyphenated because Cursor constrains `name` to "Lowercase
/// letters, numbers, and hyphens only".
pub const PREFIX: &str = "dbt-";

/// The namespaced form of `skill_name`.
///
/// Idempotent: a package that already ships `dbt-foo` installs as `dbt-foo`
/// rather than `dbt-dbt-foo`. The single prefix already marks it as dbt's.
pub fn namespaced(skill_name: &str) -> String {
    if is_dbt_owned(skill_name) {
        skill_name.to_string()
    } else {
        format!("{PREFIX}{skill_name}")
    }
}

/// Whether `dir_name` is inside dbt's namespace.
///
/// Requires something after the prefix, so a user directory named exactly
/// `dbt-` is not mistaken for a dbt install.
pub fn is_dbt_owned(dir_name: &str) -> bool {
    dir_name.len() > PREFIX.len() && dir_name.starts_with(PREFIX)
}

/// Rewrite the `name:` value in `skill_md`'s frontmatter to `new_name`.
///
/// Textual on purpose: only the value on the `name:` line changes, so the
/// author's comments, key order, quoting style and line endings all survive.
/// Parsing and re-serializing the whole frontmatter would reformat it.
pub fn rewrite_name(skill_md: &str, new_name: &str) -> FsResult<String> {
    let (head, frontmatter, tail) = split_frontmatter_parts(skill_md).ok_or_else(|| {
        fs_err!(
            ErrorCode::InvalidSkill,
            "Cannot namespace this skill: SKILL.md is missing '---' delimited frontmatter"
        )
    })?;

    let mut rewritten = String::with_capacity(frontmatter.len() + PREFIX.len());
    let mut found = false;
    for line in frontmatter.split_inclusive('\n') {
        let body = line.trim_end_matches(['\n', '\r']);
        let line_ending = &line[body.len()..];
        // Only a top-level, unindented `name:` is the skill's own name.
        if !found && body.starts_with("name:") {
            let raw = body["name:".len()..].trim();
            let quote = raw.chars().next().filter(|c| *c == '"' || *c == '\'');
            let replacement = match quote {
                Some(q) => format!("name: {q}{new_name}{q}"),
                None => format!("name: {new_name}"),
            };
            rewritten.push_str(&replacement);
            rewritten.push_str(line_ending);
            found = true;
            continue;
        }
        rewritten.push_str(line);
    }

    if !found {
        return Err(fs_err!(
            ErrorCode::InvalidSkill,
            "Cannot namespace this skill: SKILL.md frontmatter has no 'name' field"
        ));
    }

    Ok(format!("{head}{rewritten}{tail}"))
}

/// Split `contents` into (through-opening-delimiter, frontmatter, remainder).
///
/// Mirrors `validate::split_frontmatter` but returns the surrounding slices so
/// the caller can rebuild the file without touching anything else.
fn split_frontmatter_parts(contents: &str) -> Option<(&str, &str, &str)> {
    let bom_len = if contents.starts_with('\u{feff}') {
        '\u{feff}'.len_utf8()
    } else {
        0
    };
    let without_bom = &contents[bom_len..];
    let (open, rest) = if let Some(rest) = without_bom.strip_prefix("---\n") {
        ("---\n", rest)
    } else {
        ("---\r\n", without_bom.strip_prefix("---\r\n")?)
    };

    let mut offset = 0usize;
    loop {
        let end = rest[offset..]
            .find('\n')
            .map(|i| offset + i)
            .unwrap_or(rest.len());
        if rest[offset..end].trim_end_matches('\r') == "---" {
            let head_len = bom_len + open.len();
            return Some((&contents[..head_len], &rest[..offset], &rest[offset..]));
        }
        if end == rest.len() {
            return None;
        }
        offset = end + 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_skill_name_gains_the_prefix() {
        assert_eq!(namespaced("alpha"), "dbt-alpha");
    }

    #[test]
    fn prefixing_is_idempotent() {
        // A package may legitimately ship a skill already called `dbt-something`.
        // Double-prefixing would be ugly and would break the round trip, and the
        // single prefix already marks it as dbt's.
        assert_eq!(namespaced("dbt-alpha"), "dbt-alpha");
    }

    #[test]
    fn ownership_is_recognized_by_the_prefix_alone() {
        assert!(is_dbt_owned("dbt-alpha"));
        assert!(!is_dbt_owned("alpha"));
        assert!(!is_dbt_owned("dbt"));
        assert!(!is_dbt_owned("dbt-"));
        assert!(!is_dbt_owned("dbtalpha"));
    }

    #[test]
    fn rewriting_replaces_only_the_name_value() {
        let md = "---\nname: alpha\ndescription: A skill.\n---\n\n# Body\n";
        let out = rewrite_name(md, "dbt-alpha").unwrap();
        assert_eq!(
            out,
            "---\nname: dbt-alpha\ndescription: A skill.\n---\n\n# Body\n"
        );
    }

    #[test]
    fn rewriting_preserves_quoting_style_and_other_fields() {
        let md = "---\n# a comment\nname: \"alpha\"\nallowed-tools: [Read]\n---\nbody\n";
        let out = rewrite_name(md, "dbt-alpha").unwrap();
        assert!(out.contains("# a comment"), "{out}");
        assert!(out.contains("allowed-tools: [Read]"), "{out}");
        assert!(out.contains("name: \"dbt-alpha\""), "{out}");
    }

    #[test]
    fn rewriting_handles_crlf() {
        let md = "---\r\nname: alpha\r\ndescription: A skill.\r\n---\r\nbody\r\n";
        let out = rewrite_name(md, "dbt-alpha").unwrap();
        assert!(out.contains("name: dbt-alpha\r\n"), "{out:?}");
    }

    #[test]
    fn rewriting_leaves_an_indented_name_key_alone() {
        // A nested `name:` under another key is not the skill's own name.
        let md = "---\nmetadata:\n  name: inner\nname: alpha\n---\nbody\n";
        let out = rewrite_name(md, "dbt-alpha").unwrap();
        assert!(out.contains("  name: inner"), "{out}");
        assert!(out.contains("\nname: dbt-alpha"), "{out}");
    }

    #[test]
    fn rewriting_is_deterministic() {
        // Idempotency depends on regenerating byte-identical output.
        let md = "---\nname: alpha\ndescription: A skill.\n---\nbody\n";
        assert_eq!(
            rewrite_name(md, "dbt-alpha").unwrap(),
            rewrite_name(md, "dbt-alpha").unwrap()
        );
    }

    #[test]
    fn a_skill_md_with_no_name_field_is_an_error() {
        let md = "---\ndescription: A skill.\n---\nbody\n";
        assert!(rewrite_name(md, "dbt-alpha").is_err());
    }

    #[test]
    fn a_skill_md_without_frontmatter_is_an_error() {
        assert!(rewrite_name("no frontmatter\n", "dbt-alpha").is_err());
    }
}
