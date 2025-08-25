use dbt_serde_yaml::JsonSchema;
#[cfg(test)]
use fake::Dummy;
use serde::{Deserialize, Serialize};
#[cfg(test)]
use strum::EnumIter;
use strum::{EnumDiscriminants, IntoStaticStr};

#[cfg_attr(test, derive(Dummy))]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Default)]
pub struct SharedPhaseInfo {
    // Invocation id is added to all phase for consumer convenience.
    // It will always match the `invocation_id` in the root `Invocation` span.
    /// Unique identifier for the invocation
    pub invocation_id: String,
}

#[cfg_attr(test, derive(Dummy))]
#[derive(
    Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, EnumDiscriminants, strum::Display,
)]
// The following derives a variant disciriminator enum for build phases,
// used for type-safe (de)serialization and matching AND also as phase on `NodeInfo`.
// TODO: this should ideally be unified or used as the sole enum for phases in the
// scheduler and other places. Not just for telemetry data.
#[strum_discriminants(
    name(BuildPhase),
    derive(
        Serialize,
        Deserialize,
        JsonSchema,
        strum::Display,
        IntoStaticStr,
        Hash
    )
)]
#[cfg_attr(test, strum_discriminants(derive(EnumIter, Dummy)))]
// This is used to discriminate the phase data within BuildPhase which is a single
// event type in the telemetry schema.
#[serde(tag = "phase")]
pub enum BuildPhaseInfo {
    /// # File Discovery
    /// Analyzing dbt_project, profiles.yml and scanning files
    Loading {
        #[serde(flatten)]
        shared: SharedPhaseInfo,
    },

    /// # Dependency Loading
    /// Check that dependencies are met
    DependencyLoading {
        #[serde(flatten)]
        shared: SharedPhaseInfo,
    },

    /// # Parsing
    /// Parsing and macro name resolution of all dbt files
    Parsing {
        #[serde(flatten)]
        shared: SharedPhaseInfo,
    },

    /// # Scheduling
    /// Graph construction and graph slicing
    Scheduling {
        #[serde(flatten)]
        shared: SharedPhaseInfo,
    },

    /// # Freshness Analysis
    /// Freshness analysis of sources and models
    FreshnessAnalysis {
        #[serde(flatten)]
        shared: SharedPhaseInfo,
    },

    /// # Lineage
    /// Analysis of individual node lineages
    Lineage {
        #[serde(flatten)]
        shared: SharedPhaseInfo,
    },

    /// # Analyzing
    /// Dbt compile (called render) and Sql analysis
    Analyzing {
        #[serde(flatten)]
        shared: SharedPhaseInfo,
        node_count: u64,
    },

    /// # Compiling
    /// Dbt compile (called render) and Sql analysis
    Compiling {
        #[serde(flatten)]
        shared: SharedPhaseInfo,
        node_count: u64,
    },

    /// # Executing
    /// Execution against the target database
    Executing {
        #[serde(flatten)]
        shared: SharedPhaseInfo,
        node_count: u64,
    },
}
