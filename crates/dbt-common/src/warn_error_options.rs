use std::collections::BTreeMap;

use crate::collections::HashMap;
use dbt_yaml::{Value, Verbatim};
use serde::{Deserialize, Serialize};

// TODO: these models should live in dbt-schemas crate. It currently lives in dbt-common because
// EvalArgs is defined here and dbt-common cannot depend on dbt-schemas without creating a cycle.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum SupportedLegacyWarnError {}

// Serde serializes untagged unit enum variants as null, so we use a single-value
// enum here to keep `WarnErrorOptionValue::All` round-tripping as the canonical
// string "all".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum WarnErrorAllValue {
    #[serde(rename = "all", alias = "*")]
    All,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WarnErrorOptionValue {
    FusionCode(u16),
    All(WarnErrorAllValue),
    SupportedLegacy(SupportedLegacyWarnError),
    Unsupported(String),
}

impl WarnErrorOptionValue {
    pub fn all() -> Self {
        Self::All(WarnErrorAllValue::All)
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

    pub fn merge(&mut self, other: Self) {
        self.extend_error(other.error);
        self.extend_warn(other.warn);
        self.extend_silence(other.silence);
    }

    pub fn add_all_to_error(&mut self) {
        self.extend_error([WarnErrorOptionValue::all()]);
    }

    pub fn extend_error(&mut self, values: impl IntoIterator<Item = WarnErrorOptionValue>) {
        extend_unique(&mut self.error, values);
    }

    pub fn extend_warn(&mut self, values: impl IntoIterator<Item = WarnErrorOptionValue>) {
        extend_unique(&mut self.warn, values);
    }

    pub fn extend_silence(&mut self, values: impl IntoIterator<Item = WarnErrorOptionValue>) {
        extend_unique(&mut self.silence, values);
    }

    fn from_key_value_pairs<'a>(pairs: impl IntoIterator<Item = (&'a str, &'a Value)>) -> Self {
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

    Some(WarnErrorOptionValue::Unsupported(raw.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{WarnErrorOptionValue, WarnErrorOptions, parse_warn_error_options};

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
    fn merge_preserves_first_seen_order_and_deduplicates() {
        let mut left = WarnErrorOptions {
            error: vec![WarnErrorOptionValue::FusionCode(1)],
            warn: vec![],
            silence: vec![WarnErrorOptionValue::Unsupported("foo".to_string())],
            ..Default::default()
        };
        let right = WarnErrorOptions {
            error: vec![
                WarnErrorOptionValue::FusionCode(1),
                WarnErrorOptionValue::all(),
            ],
            warn: vec![WarnErrorOptionValue::Unsupported("bar".to_string())],
            silence: vec![WarnErrorOptionValue::Unsupported("foo".to_string())],
            ..Default::default()
        };

        left.merge(right);

        assert_eq!(
            left,
            WarnErrorOptions {
                error: vec![
                    WarnErrorOptionValue::FusionCode(1),
                    WarnErrorOptionValue::all(),
                ],
                warn: vec![WarnErrorOptionValue::Unsupported("bar".to_string())],
                silence: vec![WarnErrorOptionValue::Unsupported("foo".to_string())],
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
