use std::collections::BTreeMap;

use dbt_serde_yaml::JsonSchema;
use serde::{Deserialize, Serialize};
use strum::Display;

#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, JsonSchema, Default, Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum StrictnessMode {
    Baseline,
    #[default]
    Strict,
    Custom,
}

impl StrictnessMode {
    pub fn type_check(self, custom_checks: &CustomChecks) -> bool {
        match self {
            StrictnessMode::Baseline => false,
            StrictnessMode::Strict => true,
            StrictnessMode::Custom => !matches!(
                custom_checks.get("analysis.type_check"),
                Some(CustomCheckLevel::Off)
            ),
        }
    }

    /// Check if mangled ref warnings should be emitted.
    /// By default enabled in Strict mode, can be disabled via custom_checks in any mode.
    pub fn mangled_ref_check(self, custom_checks: &CustomChecks) -> bool {
        // If explicitly disabled via custom_checks, always respect it
        if matches!(
            custom_checks.get("analysis.mangled_ref"),
            Some(CustomCheckLevel::Off)
        ) {
            return false;
        }
        match self {
            StrictnessMode::Baseline => false,
            StrictnessMode::Strict | StrictnessMode::Custom => true,
        }
    }
}

#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, JsonSchema, Default, Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum CustomCheckLevel {
    Error,
    Warn,
    Advisory,
    #[default]
    Off,
}

pub type CustomChecks = BTreeMap<String, CustomCheckLevel>;
