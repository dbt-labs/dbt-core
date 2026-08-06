//! `ai_provider` resolution and the provider → destination-directory map.

use std::collections::BTreeSet;
use std::path::PathBuf;

use dbt_common::ErrorCode;
use dbt_common::tracing::dbt_emit::emit_warn_log_message;
use dbt_common::warn_error_options::project_flags_get_value;

/// The de-facto cross-tool skills directory. Read by dbt Wizard, Codex, Cursor
/// and Gemini CLI; the default for any known provider that isn't Claude Code.
pub const DEFAULT_SKILLS_DIR: &str = ".agents/skills";
/// Claude Code only discovers skills under its own directory.
pub const CLAUDE_SKILLS_DIR: &str = ".claude/skills";

/// Providers dbt knows how to install for, and where each one reads skills from.
///
/// Kept data-driven so adding a provider is a one-line change.
const PROVIDER_DESTINATIONS: &[(&str, &str)] = &[
    ("wizard", DEFAULT_SKILLS_DIR),
    ("claude", CLAUDE_SKILLS_DIR),
    ("openai", DEFAULT_SKILLS_DIR),
    ("codex", DEFAULT_SKILLS_DIR),
    ("cursor", DEFAULT_SKILLS_DIR),
    ("gemini", DEFAULT_SKILLS_DIR),
];

/// The destination directory for a provider, or `None` if dbt doesn't know it.
pub fn provider_destination(provider: &str) -> Option<&'static str> {
    let provider = provider.trim().to_ascii_lowercase();
    PROVIDER_DESTINATIONS
        .iter()
        .find(|(name, _)| *name == provider)
        .map(|(_, dest)| *dest)
}

/// Every provider dbt knows about, for use in warning messages.
pub fn known_providers() -> Vec<&'static str> {
    PROVIDER_DESTINATIONS
        .iter()
        .map(|(name, _)| *name)
        .collect()
}

/// Map `ai_provider` values to destination directories relative to the project root.
///
/// Unknown providers WARN and contribute nothing. Providers that share a
/// destination (the common case — everything except `claude` writes to
/// `.agents/skills/`) are deduplicated, so a skill is written once per distinct
/// directory rather than once per provider.
pub fn resolve_destinations(providers: &[String]) -> Vec<PathBuf> {
    let mut seen = BTreeSet::new();
    let mut destinations = Vec::new();

    for provider in providers {
        match provider_destination(provider) {
            Some(dest) => {
                if seen.insert(dest) {
                    destinations.push(PathBuf::from(dest));
                }
            }
            None => emit_warn_log_message(
                ErrorCode::UnknownAiProvider,
                format!(
                    "Unknown ai_provider '{}'; no skills will be installed for it. Known providers: {}.",
                    provider.trim(),
                    known_providers().join(", ")
                ),
            ),
        }
    }

    destinations
}

/// Resolve `ai_provider` from the CLI/env value and the project's `flags:` block.
///
/// CLI (which clap also populates from `DBT_AI_PROVIDER`) wins over
/// `dbt_project.yml`, matching how every other dual-source flag resolves.
/// Accepts either a single string or a list in the project file.
pub fn resolve_ai_provider(
    from_cli: Option<&[String]>,
    project_flags: Option<&dbt_yaml::Value>,
) -> Option<Vec<String>> {
    if let Some(from_cli) = from_cli
        && !from_cli.is_empty()
    {
        return Some(from_cli.to_vec());
    }

    let value = project_flags.and_then(|flags| project_flags_get_value(flags, "ai_provider"))?;
    let providers = match value {
        dbt_yaml::Value::Sequence(values, _) => values
            .iter()
            .filter_map(|value| value.as_str().map(str::to_string))
            .collect(),
        other => other.as_str().map(|s| vec![s.to_string()])?,
    };

    let providers: Vec<String> = providers
        .into_iter()
        .filter(|provider: &String| !provider.trim().is_empty())
        .collect();

    (!providers.is_empty()).then_some(providers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claude_gets_its_own_dir_and_everything_else_shares_agents() {
        assert_eq!(provider_destination("claude"), Some(CLAUDE_SKILLS_DIR));
        assert_eq!(provider_destination("wizard"), Some(DEFAULT_SKILLS_DIR));
        assert_eq!(provider_destination("codex"), Some(DEFAULT_SKILLS_DIR));
        assert_eq!(provider_destination("nope"), None);
    }

    #[test]
    fn provider_lookup_is_case_and_whitespace_insensitive() {
        assert_eq!(provider_destination("  Claude "), Some(CLAUDE_SKILLS_DIR));
    }

    #[test]
    fn destinations_are_deduplicated() {
        let providers = ["wizard", "codex", "cursor", "claude"].map(String::from);
        assert_eq!(
            resolve_destinations(&providers),
            vec![
                PathBuf::from(DEFAULT_SKILLS_DIR),
                PathBuf::from(CLAUDE_SKILLS_DIR)
            ]
        );
    }

    #[test]
    fn unknown_provider_contributes_nothing() {
        let providers = ["definitely-not-a-harness".to_string()];
        assert!(resolve_destinations(&providers).is_empty());
    }

    #[test]
    fn cli_wins_over_project_flags() {
        let flags: dbt_yaml::Value = dbt_yaml::from_str("ai_provider: claude").unwrap();
        let from_cli = ["wizard".to_string()];
        assert_eq!(
            resolve_ai_provider(Some(&from_cli), Some(&flags)),
            Some(vec!["wizard".to_string()])
        );
    }

    #[test]
    fn project_flags_accept_a_bare_string() {
        let flags: dbt_yaml::Value = dbt_yaml::from_str("ai_provider: claude").unwrap();
        assert_eq!(
            resolve_ai_provider(None, Some(&flags)),
            Some(vec!["claude".to_string()])
        );
    }

    #[test]
    fn project_flags_accept_a_list() {
        let flags: dbt_yaml::Value =
            dbt_yaml::from_str("ai_provider:\n  - claude\n  - wizard\n").unwrap();
        assert_eq!(
            resolve_ai_provider(None, Some(&flags)),
            Some(vec!["claude".to_string(), "wizard".to_string()])
        );
    }

    #[test]
    fn unset_everywhere_resolves_to_none() {
        assert_eq!(resolve_ai_provider(None, None), None);
        let flags: dbt_yaml::Value = dbt_yaml::from_str("something_else: true").unwrap();
        assert_eq!(resolve_ai_provider(Some(&[]), Some(&flags)), None);
    }
}
