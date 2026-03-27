use std::collections::BTreeMap;

use crate::ErrorCode;
use crate::collections::HashMap;
use dbt_yaml::{Value, Verbatim};
use serde::{Deserialize, Serialize};
use strum::EnumString;

// TODO: these models should live in dbt-schemas crate. It currently lives in dbt-common because
// EvalArgs is defined here and dbt-common cannot depend on dbt-schemas without creating a cycle.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, EnumString,
)]
pub enum SupportedLegacyWarnError {
    NothingToDo,
    NoNodesSelected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum WarnErrorGroupValue {
    #[serde(rename = "all", alias = "*")]
    All,
    #[serde(rename = "Deprecations")]
    Deprecations,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WarnErrorOptionValue {
    FusionCode(u16),
    LegacyGroup(WarnErrorGroupValue),
    SupportedLegacy(SupportedLegacyWarnError),
    Unsupported(String),
}

impl WarnErrorOptionValue {
    pub fn all() -> Self {
        Self::LegacyGroup(WarnErrorGroupValue::All)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WarnErrorOptions {
    #[serde(default)]
    pub error: Vec<WarnErrorOptionValue>,
    #[serde(default)]
    pub warn: Vec<WarnErrorOptionValue>,
    #[serde(default)]
    pub silence: Vec<WarnErrorOptionValue>,
    #[serde(default, skip_serializing)]
    pub __ignored__: Verbatim<HashMap<String, Value>>,
}

/// Decisions order in their precedence order, from lowest to highest:
/// - Upgrade the warning to an error
/// - Retain the warning as is
/// - Silence the warning entirely
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum WarnErrorDecision {
    UpgradeToError,
    Retain,
    Silence,
}

// Parsing related implementations. For resolving logic see separate block below
impl WarnErrorOptions {
    pub fn from_cli_mapping(mapping: BTreeMap<String, Value>) -> Self {
        Self::from_key_value_pairs(mapping.iter().map(|(key, value)| (key.as_str(), value)))
    }

    pub fn from_yaml_value(value: &Value) -> Self {
        let Value::Mapping(mapping, _) = value else {
            return Self::default();
        };

        Self::from_key_value_pairs(
            mapping
                .iter()
                .filter_map(|(key, value)| key.as_str().map(|key| (key, value))),
        )
    }

    pub fn add_all_to_error(&mut self) {
        self.extend_error([WarnErrorOptionValue::all()]);
    }

    pub fn has_unsupported_values(&self) -> bool {
        self.error
            .iter()
            .chain(self.warn.iter())
            .chain(self.silence.iter())
            .any(|value| {
                matches!(
                    value,
                    WarnErrorOptionValue::Unsupported(_) | WarnErrorOptionValue::LegacyGroup(_)
                ) || matches!(
                    value,
                    // Here we are super conservative. In reality we would match all error codes,
                    // but since some of them may happen in places where user would expect short-circuiting
                    // of execution and we only currently explicitly checkpoin after the following codes
                    // we would emit the unsupported warning for all other codes for now
                    WarnErrorOptionValue::FusionCode(c) if *c != ErrorCode::NoNodesSelected as u16
                )
            })
    }

    fn extend_error(&mut self, values: impl IntoIterator<Item = WarnErrorOptionValue>) {
        extend_unique(&mut self.error, values);
    }

    fn extend_warn(&mut self, values: impl IntoIterator<Item = WarnErrorOptionValue>) {
        extend_unique(&mut self.warn, values);
    }

    fn extend_silence(&mut self, values: impl IntoIterator<Item = WarnErrorOptionValue>) {
        extend_unique(&mut self.silence, values);
    }

    fn from_key_value_pairs<'a>(pairs: impl IntoIterator<Item = (&'a str, &'a Value)>) -> Self {
        // TODO: we currently allow `warn` to be present even when `All` or `Deprecations` are not
        // present in `error` which is not matching dbt-core which errors. But resolution logic
        // is still honoring precedence correctly
        let mut warn_error_options = Self::default();

        for (key, value) in pairs {
            match key.to_ascii_lowercase().as_str() {
                "include" | "error" => {
                    warn_error_options.extend_error(parse_warn_error_option_values(value));
                }
                "warn" => {
                    warn_error_options.extend_warn(parse_warn_error_option_values(value));
                }
                "exclude" | "silence" => {
                    warn_error_options.extend_silence(parse_warn_error_option_values(value));
                }
                _ => {}
            }
        }

        warn_error_options
    }
}

pub fn parse_warn_error_options(value: &str) -> Result<WarnErrorOptions, String> {
    crate::io_args::check_var(value).map(WarnErrorOptions::from_cli_mapping)
}

pub fn resolve_warn_error_options(
    from_cli: Option<bool>,
    from_cli_or_env: Option<&WarnErrorOptions>,
    project_flags: Option<&Value>,
) -> (bool, WarnErrorOptions) {
    let warn_error = from_cli.unwrap_or_else(|| {
        project_flags
            .and_then(project_flags_warn_error)
            .unwrap_or_default()
    });
    let mut warn_error_options = from_cli_or_env.cloned().unwrap_or_else(|| {
        project_flags
            .and_then(project_flags_warn_error_options)
            .unwrap_or_default()
    });
    if warn_error {
        warn_error_options.add_all_to_error();
    }
    (warn_error, warn_error_options)
}

fn project_flags_warn_error(flags: &Value) -> Option<bool> {
    project_flags_get_value(flags, "warn_error").and_then(Value::as_bool)
}

fn project_flags_warn_error_options(flags: &Value) -> Option<WarnErrorOptions> {
    project_flags_get_value(flags, "warn_error_options").map(WarnErrorOptions::from_yaml_value)
}

fn project_flags_get_value<'a>(flags: &'a Value, key: &str) -> Option<&'a Value> {
    let Value::Mapping(mapping, _) = flags else {
        return None;
    };

    mapping.iter().find_map(|(candidate, value)| {
        candidate
            .as_str()
            .filter(|candidate| candidate.eq_ignore_ascii_case(key))
            .map(|_| value)
    })
}

fn extend_unique(
    target: &mut Vec<WarnErrorOptionValue>,
    values: impl IntoIterator<Item = WarnErrorOptionValue>,
) {
    for value in values {
        if !target.contains(&value) {
            target.push(value);
        }
    }
}

fn parse_warn_error_option_values(value: &Value) -> Vec<WarnErrorOptionValue> {
    match value {
        Value::Sequence(values, _) => values
            .iter()
            .filter_map(parse_warn_error_option_value)
            .collect(),
        _ => parse_warn_error_option_value(value).into_iter().collect(),
    }
}

fn parse_warn_error_option_value(value: &Value) -> Option<WarnErrorOptionValue> {
    if let Some(code) = value.as_u64().and_then(|value| u16::try_from(value).ok()) {
        return Some(WarnErrorOptionValue::FusionCode(code));
    }

    let raw = value.as_str()?;
    if raw == "*" || raw.eq_ignore_ascii_case("all") {
        return Some(WarnErrorOptionValue::all());
    }

    if raw.eq_ignore_ascii_case("Deprecations") {
        return Some(WarnErrorOptionValue::LegacyGroup(
            WarnErrorGroupValue::Deprecations,
        ));
    }

    if let Ok(legacy) = raw.try_into() {
        return Some(WarnErrorOptionValue::SupportedLegacy(legacy));
    }

    Some(WarnErrorOptionValue::Unsupported(raw.to_string()))
}

impl WarnErrorOptions {
    pub fn decision_for_error_code(&self, error_code: ErrorCode) -> WarnErrorDecision {
        if error_code == ErrorCode::NotSupportedWarnErrorOption {
            // This is a special case for when we encounter an error code that we know is not supported by the current options.
            // In this case we want to emit the warning about unsupported code, but we don't want to apply any silencing or upgrading logic to it.
            // Otherwise we may exit so early even manifest is not available, which breaks tests & possibly
            // some downstream assumption
            return WarnErrorDecision::Retain;
        }

        // dbt-core precedence logic is as follows:
        // 1. named event > Deprecations > "all" / "*"
        // 2. silence > warn > error

        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
        enum MatchType {
            NotMatched,
            All, // least specific match, matches all error codes
            Deprecation,
            NamedEvent,
        }

        let matches = |value: &WarnErrorOptionValue| match value {
            WarnErrorOptionValue::LegacyGroup(WarnErrorGroupValue::All) => MatchType::All,
            WarnErrorOptionValue::LegacyGroup(WarnErrorGroupValue::Deprecations) => {
                MatchType::Deprecation
            }
            WarnErrorOptionValue::FusionCode(code) if *code == error_code as u16 => {
                MatchType::NamedEvent
            }
            WarnErrorOptionValue::SupportedLegacy(legacy)
                if matches_legacy_error_code(*legacy, error_code) =>
            {
                MatchType::NamedEvent
            }
            _ => MatchType::NotMatched,
        };

        let max_matches_and_verdict = [
            (
                self.silence
                    .iter()
                    .map(matches)
                    .max()
                    .unwrap_or(MatchType::NotMatched),
                WarnErrorDecision::Silence,
            ),
            (
                self.warn
                    .iter()
                    .map(matches)
                    .max()
                    .unwrap_or(MatchType::NotMatched),
                WarnErrorDecision::Retain,
            ),
            (
                self.error
                    .iter()
                    .map(matches)
                    .max()
                    .unwrap_or(MatchType::NotMatched),
                WarnErrorDecision::UpgradeToError,
            ),
        ];

        max_matches_and_verdict
            .into_iter()
            // Do not consider options that do not match the error code at all
            .filter(|(match_type, _verdict)| *match_type != MatchType::NotMatched)
            // Order first by most specific match type, then by verdict precedence (silence > warn > error)
            .max()
            // If one exist, then this is the verdict we return
            .map(|(_match_type, verdict)| verdict)
            // default to retaining the warning if no matches at all, including "all"
            .unwrap_or(WarnErrorDecision::Retain)
    }
}

fn matches_legacy_error_code(legacy: SupportedLegacyWarnError, error_code: ErrorCode) -> bool {
    match legacy {
        SupportedLegacyWarnError::NothingToDo | SupportedLegacyWarnError::NoNodesSelected => {
            error_code == ErrorCode::NoNodesSelected
        }
    }
}

// Tests here are only for things not covered with e2e dbt-cli tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorCode;

    #[test]
    fn error_decision_honors_precedence() {
        // TODO: we can't test deprecations group precedence yet as we have no mapping of our
        // error codes to dbt-core deprecations.
        let options = WarnErrorOptions {
            error: vec![
                WarnErrorOptionValue::FusionCode(ErrorCode::Generic as u16),
                WarnErrorOptionValue::FusionCode(ErrorCode::NoNodesSelected as u16),
                WarnErrorOptionValue::FusionCode(ErrorCode::IoError as u16),
                WarnErrorOptionValue::LegacyGroup(WarnErrorGroupValue::All),
            ],
            warn: vec![
                // This one in silence, so silence should win
                WarnErrorOptionValue::FusionCode(ErrorCode::NoNodesSelected as u16),
                // This one should win over error because it's a specific match,
                // neither All, nor direct code should override it
                WarnErrorOptionValue::FusionCode(ErrorCode::IoError as u16),
            ],
            silence: vec![WarnErrorOptionValue::FusionCode(
                ErrorCode::NoNodesSelected as u16,
            )],
            ..Default::default()
        };

        assert_eq!(
            options.decision_for_error_code(ErrorCode::NoNodesSelected),
            WarnErrorDecision::Silence,
            "Specific code match in silence should take precedence over all matches in warn and error, including specific matches"
        );

        assert_eq!(
            options.decision_for_error_code(ErrorCode::JsonError),
            WarnErrorDecision::UpgradeToError,
            "All group in error should take precedence over all match in warn and all in silence"
        );

        assert_eq!(
            options.decision_for_error_code(ErrorCode::IoError),
            WarnErrorDecision::Retain,
            "Specific code match in warn should take precedence over all matches in error"
        );
    }

    #[test]
    fn parses_legacy_cli_shape_case_insensitively_and_deduplicates() {
        let parsed = parse_warn_error_options(
            "{InClUdE: [all, '*', ALL, BogusWarningClass, BogusWarningClass, 17], ExClUdE: [foo, foo]}",
        )
        .unwrap();

        assert_eq!(
            parsed,
            WarnErrorOptions {
                error: vec![
                    WarnErrorOptionValue::all(),
                    WarnErrorOptionValue::Unsupported("BogusWarningClass".to_string()),
                    WarnErrorOptionValue::FusionCode(17),
                ],
                warn: vec![],
                silence: vec![WarnErrorOptionValue::Unsupported("foo".to_string())],
                ..Default::default()
            }
        );
    }

    #[test]
    fn parses_v2_shape_and_ignores_unknown_keys() {
        let parsed = WarnErrorOptions::from_yaml_value(
            &dbt_yaml::from_str(
                "{error: [1, all], WARN: NoNodesForSelectionCriteria, silence: [x, x], bogus: [2]}",
            )
            .unwrap(),
        );

        assert_eq!(
            parsed,
            WarnErrorOptions {
                error: vec![
                    WarnErrorOptionValue::FusionCode(1),
                    WarnErrorOptionValue::all()
                ],
                warn: vec![WarnErrorOptionValue::Unsupported(
                    "NoNodesForSelectionCriteria".to_string(),
                )],
                silence: vec![WarnErrorOptionValue::Unsupported("x".to_string())],
                ..Default::default()
            }
        );
    }

    #[test]
    fn serializes_all_canonically() {
        let options = WarnErrorOptions {
            error: vec![WarnErrorOptionValue::all()],
            ..Default::default()
        };

        assert_eq!(
            serde_json::to_value(&options).unwrap(),
            serde_json::json!({
                "error": ["all"],
                "warn": [],
                "silence": [],
            })
        );
    }
}
