//! dbt's bookkeeping inside the installed `SKILL.md`'s `metadata` frontmatter.
//!
//! The AgentSkills spec allows exactly six frontmatter fields and reserves
//! `metadata` for client-specific data; Claude Code does not act on its
//! contents. So dbt records what it installed there rather than in a sidecar
//! file, keeping the skill directory to exactly the files the package shipped.
//!
//! Only the installed copy is ever written. A package's own `SKILL.md` is read
//! and never modified.

use dbt_common::{ErrorCode, FsResult, fs_err};
use indexmap::IndexMap;

/// Marks a `metadata` map as dbt's. dbt ignores skills without it.
pub const MANAGED_BY_KEY: &str = "dbt.managed_by";
const MANAGED_BY_VALUE: &str = "dbt";
const SOURCE_KEY: &str = "dbt.source";
const PACKAGE_KEY: &str = "dbt.package";
const VERSION_KEY: &str = "dbt.version";
const SOURCE_PATH_KEY: &str = "dbt.source_path";
const SOURCE_HASH_KEY: &str = "dbt.source_hash";
const INSTALLED_AT_KEY: &str = "dbt.installed_at";
const SHADOWED_KEY: &str = "dbt.shadowed";

/// Separator for the `dbt.shadowed` list. `metadata` values must be strings, so
/// the shadowed set is joined rather than emitted as a YAML sequence.
const SHADOWED_SEPARATOR: &str = ",";

/// What dbt records about a skill it installed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillMetadata {
    /// `package` or `project`.
    pub source: String,
    /// `None` when the skill came from the project itself.
    pub package: Option<String>,
    pub version: Option<String>,
    pub source_path: String,
    /// `sha256:…` over the source directory at install time.
    pub source_hash: String,
    pub installed_at: String,
    /// `<package>:<source_path>` for each skill that lost a name collision.
    pub shadowed: Vec<String>,
}

impl SkillMetadata {
    fn to_pairs(&self) -> Vec<(&'static str, String)> {
        let mut pairs = vec![
            (MANAGED_BY_KEY, MANAGED_BY_VALUE.to_string()),
            (SOURCE_KEY, self.source.clone()),
        ];
        if let Some(package) = &self.package {
            pairs.push((PACKAGE_KEY, package.clone()));
        }
        if let Some(version) = &self.version {
            pairs.push((VERSION_KEY, version.clone()));
        }
        pairs.push((SOURCE_PATH_KEY, self.source_path.clone()));
        pairs.push((SOURCE_HASH_KEY, self.source_hash.clone()));
        pairs.push((INSTALLED_AT_KEY, self.installed_at.clone()));
        if !self.shadowed.is_empty() {
            pairs.push((SHADOWED_KEY, self.shadowed.join(SHADOWED_SEPARATOR)));
        }
        pairs
    }
}

/// Split `contents` into (through-opening-delimiter, frontmatter, remainder).
///
/// Mirrors `validate::split_frontmatter` but returns the surrounding slices so
/// the caller can rebuild the file without reformatting anything it did not
/// touch.
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

/// Write dbt's keys into `skill_md`'s `metadata` map, returning the new file.
///
/// Textual, not a parse-and-reserialize: everything outside the `metadata:`
/// block keeps its exact bytes, so author comments and key order survive. An
/// author's own `metadata` entries are preserved by value; only `dbt.*` keys
/// belong to dbt.
pub fn inject(skill_md: &str, meta: &SkillMetadata) -> FsResult<String> {
    let (head, frontmatter, tail) = split_frontmatter_parts(skill_md).ok_or_else(|| {
        fs_err!(
            ErrorCode::InvalidSkill,
            "Cannot record dbt metadata: SKILL.md is missing '---' delimited frontmatter"
        )
    })?;

    if has_unsupported_metadata_shape(frontmatter) {
        return Err(fs_err!(
            ErrorCode::InvalidSkill,
            "Cannot record dbt metadata: SKILL.md declares 'metadata' in a form dbt cannot \
             rewrite safely (a multi-line flow mapping). Use a block mapping instead."
        ));
    }

    let authored = existing_metadata(frontmatter)?;
    let mut merged: IndexMap<String, String> = authored
        .into_iter()
        .filter(|(key, _)| !key.starts_with("dbt."))
        .collect();
    for (key, value) in meta.to_pairs() {
        merged.insert(key.to_string(), value);
    }

    let mut block = String::from("metadata:\n");
    for (key, value) in &merged {
        // Quote every value: hashes and timestamps both contain ':', which is
        // ambiguous unquoted in YAML.
        block.push_str(&format!("  {key}: \"{value}\"\n"));
    }

    let stripped = strip_metadata_block(frontmatter);
    Ok(format!("{head}{stripped}{block}{tail}"))
}

/// Read dbt's keys back, or `None` when this skill is not dbt's.
///
/// A skill with no `metadata`, no `dbt.managed_by`, or unparseable frontmatter
/// is not ours to touch, so all three collapse to `None` rather than an error.
pub fn read(skill_md: &str) -> Option<SkillMetadata> {
    let (_, frontmatter, _) = split_frontmatter_parts(skill_md)?;
    let map = existing_metadata(frontmatter).ok()?;
    if map.get(MANAGED_BY_KEY).map(String::as_str) != Some(MANAGED_BY_VALUE) {
        return None;
    }
    Some(SkillMetadata {
        source: map.get(SOURCE_KEY).cloned().unwrap_or_default(),
        package: map.get(PACKAGE_KEY).cloned(),
        version: map.get(VERSION_KEY).cloned(),
        source_path: map.get(SOURCE_PATH_KEY).cloned().unwrap_or_default(),
        source_hash: map.get(SOURCE_HASH_KEY).cloned().unwrap_or_default(),
        installed_at: map.get(INSTALLED_AT_KEY).cloned().unwrap_or_default(),
        shadowed: map
            .get(SHADOWED_KEY)
            .map(|joined| {
                joined
                    .split(SHADOWED_SEPARATOR)
                    .filter(|entry| !entry.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default(),
    })
}

/// The `metadata` map already in this frontmatter, flattened to strings.
fn existing_metadata(frontmatter: &str) -> FsResult<IndexMap<String, String>> {
    let parsed: dbt_yaml::Value = dbt_yaml::from_str(frontmatter).map_err(|e| {
        fs_err!(
            ErrorCode::InvalidSkill,
            "Cannot record dbt metadata: unparseable SKILL.md frontmatter: {}",
            e
        )
    })?;
    let Some(mapping) = parsed.as_mapping() else {
        return Ok(IndexMap::new());
    };
    let Some((_, value)) = mapping
        .iter()
        .find(|(key, _)| key.as_str() == Some("metadata"))
    else {
        return Ok(IndexMap::new());
    };
    let Some(entries) = value.as_mapping() else {
        // Claude Code drops a non-map `metadata`; treat it as absent.
        return Ok(IndexMap::new());
    };
    Ok(entries
        .iter()
        .filter_map(|(key, value)| Some((key.as_str()?.to_string(), value.as_str()?.to_string())))
        .collect())
}

/// Whether `metadata` is declared in a shape `strip_metadata_block` would
/// mangle: a flow mapping spread over more than one line.
fn has_unsupported_metadata_shape(frontmatter: &str) -> bool {
    for line in frontmatter.lines() {
        let trimmed = line.trim_end_matches('\r');
        if let Some(rest) = trimmed.strip_prefix("metadata:") {
            let rest = rest.trim();
            return rest.starts_with('{') && !rest.ends_with('}');
        }
    }
    false
}

/// Remove a top-level `metadata:` mapping from `frontmatter`.
///
/// Only that one declaration is rewritten; every other line keeps its bytes.
/// Handles a block mapping and a single-line flow mapping; a multi-line flow
/// mapping is rejected earlier by `has_unsupported_metadata_shape`.
fn strip_metadata_block(frontmatter: &str) -> String {
    let mut out = String::new();
    let mut skipping_block = false;
    for line in frontmatter.split_inclusive('\n') {
        let trimmed = line.trim_end_matches(['\n', '\r']);
        if skipping_block {
            // Continuation lines of a block mapping are indented; anything else
            // ends it.
            if trimmed.is_empty() || trimmed.starts_with(' ') || trimmed.starts_with('\t') {
                continue;
            }
            skipping_block = false;
        }
        if trimmed == "metadata:" {
            skipping_block = true;
            continue;
        }
        if trimmed.starts_with("metadata: ") {
            // Single-line flow mapping or scalar; drop just this line.
            continue;
        }
        out.push_str(line);
    }
    if !out.is_empty() && !out.ends_with('\n') {
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> SkillMetadata {
        SkillMetadata {
            source: "package".to_string(),
            package: Some("dbt_project_evaluator".to_string()),
            version: Some("1.5.0".to_string()),
            source_path: "skills/how-to".to_string(),
            source_hash: "sha256:abc".to_string(),
            installed_at: "2026-06-30T00:00:00Z".to_string(),
            shadowed: vec![],
        }
    }

    const PLAIN: &str = "---\nname: foo\ndescription: A skill.\n---\n\n# Body\n";

    #[test]
    fn injects_a_metadata_block_and_reads_it_back() {
        let injected = inject(PLAIN, &sample()).unwrap();
        assert_eq!(read(&injected), Some(sample()));
    }

    #[test]
    fn injection_preserves_the_authors_other_frontmatter_and_body_verbatim() {
        let injected = inject(PLAIN, &sample()).unwrap();
        assert!(injected.contains("name: foo"), "{injected}");
        assert!(injected.contains("description: A skill."), "{injected}");
        assert!(injected.ends_with("\n# Body\n"), "{injected}");
        // Only the six spec fields may appear at the top level.
        assert!(
            !injected.contains("\ndbt."),
            "no top-level dbt keys: {injected}"
        );
    }

    #[test]
    fn a_skill_without_our_keys_is_not_ours() {
        assert_eq!(read(PLAIN), None);
    }

    #[test]
    fn someone_elses_metadata_is_not_ours() {
        let other = "---\nname: foo\ndescription: A skill.\nmetadata:\n  vendor.tool: other\n---\n";
        assert_eq!(read(other), None);
    }

    #[test]
    fn an_authors_existing_metadata_keys_survive_injection() {
        let authored = "---\nname: foo\ndescription: A skill.\nmetadata:\n  vendor.tier: gold\n---\n\n# Body\n";
        let injected = inject(authored, &sample()).unwrap();
        assert!(injected.contains("vendor.tier"), "{injected}");
        assert_eq!(read(&injected), Some(sample()));
    }

    #[test]
    fn shadowed_round_trips_through_a_joined_string() {
        let mut meta = sample();
        meta.shadowed = vec!["some_pkg:skills/shared".to_string()];
        let injected = inject(PLAIN, &meta).unwrap();
        assert_eq!(read(&injected), Some(meta));
    }

    #[test]
    fn injection_is_deterministic() {
        // Idempotency depends on regenerating byte-identical output.
        assert_eq!(
            inject(PLAIN, &sample()).unwrap(),
            inject(PLAIN, &sample()).unwrap()
        );
    }

    #[test]
    fn re_injecting_replaces_dbts_own_keys_rather_than_duplicating_them() {
        let once = inject(PLAIN, &sample()).unwrap();
        let twice = inject(&once, &sample()).unwrap();
        assert_eq!(once, twice);
    }

    #[test]
    fn a_skill_md_without_frontmatter_is_an_error() {
        assert!(inject("no frontmatter here\n", &sample()).is_err());
    }

    #[test]
    fn a_multiline_flow_metadata_mapping_is_refused_rather_than_mangled() {
        let awkward = "---\nname: foo\nmetadata: {\n  a: b\n}\n---\nbody\n";
        assert!(inject(awkward, &sample()).is_err());
    }

    #[test]
    fn allowed_tools_survives_injection() {
        let with_tools =
            "---\nname: foo\ndescription: A skill.\nallowed-tools: [Read, Grep]\n---\nbody\n";
        let injected = inject(with_tools, &sample()).unwrap();
        assert!(
            injected.contains("allowed-tools: [Read, Grep]"),
            "{injected}"
        );
        assert_eq!(read(&injected), Some(sample()));
    }
}
