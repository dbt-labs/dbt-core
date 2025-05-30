use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::schemas::manifest::InternalDbtNode;
use crate::{
    dbt_utils::get_dbt_schema_version,
    schemas::{
        common::DbtQuoting,
        macros::{DbtDocsMacro, DbtMacro},
    },
    state::ResolverState,
};

use super::{
    DbtExposure, DbtGroup, DbtMetric, DbtModel, DbtOperation, DbtSavedQuery, DbtSeed, DbtSelector,
    DbtSemanticModel, DbtSnapshot, DbtSource, DbtTest, DbtUnitTest,
};

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "resource_type")]
#[serde(rename_all = "snake_case")]
pub enum DbtNode {
    Model(DbtModel),
    Test(DbtTest),
    Snapshot(DbtSnapshot),
    Seed(DbtSeed),
    Operation(DbtOperation),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ManifestMetadata {
    #[serde(flatten)]
    pub base: BaseMetadata,
    #[serde(default)]
    pub project_name: String,
    pub project_id: Option<String>,
    pub user_id: Option<String>,
    pub send_anonymous_usage_stats: Option<bool>,
    #[serde(default)]
    pub adapter_type: String,
    pub quoting: Option<DbtQuoting>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BaseMetadata {
    pub dbt_schema_version: String,
    pub dbt_version: String,
    pub generated_at: DateTime<Utc>,
    pub invocation_id: Option<String>,
    pub invocation_started_at: Option<DateTime<Utc>>,
    pub env: BTreeMap<String, String>,
}

impl PartialEq for ManifestMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.base.env == other.base.env
            && self.project_name == other.project_name
            && self.send_anonymous_usage_stats == other.send_anonymous_usage_stats
            && self.adapter_type == other.adapter_type
        // Note: We intentionally skip comparing the following right now:
        // - generated_at (timestamp)
        // - invocation_id (changes each run)
        // - user_id (may change between environments)
        // - dbt_schema_version (changes between versions)
        // - dbt_version (changes between versions)
        // - project_id (changes between environments)
    }
}

impl Eq for ManifestMetadata {}

/// External representation of the manifest, internal we use Nodes
#[derive(Debug, Default, Deserialize)]
pub struct DbtManifest {
    pub metadata: ManifestMetadata,
    pub nodes: BTreeMap<String, DbtNode>,
    pub sources: BTreeMap<String, DbtSource>,
    pub macros: BTreeMap<String, DbtMacro>,
    pub unit_tests: BTreeMap<String, DbtUnitTest>,
    pub docs: BTreeMap<String, DbtDocsMacro>,
    pub semantic_models: BTreeMap<String, DbtSemanticModel>,
    pub saved_queries: BTreeMap<String, DbtSavedQuery>,
    pub exposures: BTreeMap<String, DbtExposure>,
    pub metrics: BTreeMap<String, DbtMetric>,
    pub child_map: BTreeMap<String, Vec<String>>,
    pub parent_map: BTreeMap<String, Vec<String>>,
    pub group_map: BTreeMap<String, Vec<String>>,
    pub disabled: BTreeMap<String, Vec<serde_json::Value>>,
    pub selectors: BTreeMap<String, DbtSelector>,
    pub groups: BTreeMap<String, DbtGroup>,
}

impl Serialize for DbtManifest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = BTreeMap::new();
        map.insert(
            "metadata".to_string(),
            serde_json::to_value(&self.metadata).map_err(serde::ser::Error::custom)?,
        );
        map.insert(
            "nodes".to_string(),
            serde_json::to_value(&self.nodes).map_err(serde::ser::Error::custom)?,
        );

        // Serialize sources using InternalDbtNode trait
        let sources_serialized: BTreeMap<String, serde_json::Value> = self
            .sources
            .iter()
            .map(|(k, v)| (k.clone(), InternalDbtNode::serialize(v)))
            .collect();
        map.insert(
            "sources".to_string(),
            serde_json::to_value(sources_serialized).map_err(serde::ser::Error::custom)?,
        );

        // Serialize macros using InternalDbtNode trait
        let macros_serialized: BTreeMap<String, serde_json::Value> = self
            .macros
            .iter()
            .map(|(k, v)| (k.clone(), InternalDbtNode::serialize(v)))
            .collect();
        map.insert(
            "macros".to_string(),
            serde_json::to_value(macros_serialized).map_err(serde::ser::Error::custom)?,
        );

        // Serialize unit_tests using InternalDbtNode trait
        let unit_tests_serialized: BTreeMap<String, serde_json::Value> = self
            .unit_tests
            .iter()
            .map(|(k, v)| (k.clone(), InternalDbtNode::serialize(v)))
            .collect();
        map.insert(
            "unit_tests".to_string(),
            serde_json::to_value(unit_tests_serialized).map_err(serde::ser::Error::custom)?,
        );

        map.insert(
            "docs".to_string(),
            serde_json::to_value(&self.docs).map_err(serde::ser::Error::custom)?,
        );

        // Serialize semantic_models using InternalDbtNode trait
        let semantic_models_serialized: BTreeMap<String, serde_json::Value> = self
            .semantic_models
            .iter()
            .map(|(k, v)| (k.clone(), InternalDbtNode::serialize(v)))
            .collect();
        map.insert(
            "semantic_models".to_string(),
            serde_json::to_value(semantic_models_serialized).map_err(serde::ser::Error::custom)?,
        );

        // Serialize saved_queries using InternalDbtNode trait
        let saved_queries_serialized: BTreeMap<String, serde_json::Value> = self
            .saved_queries
            .iter()
            .map(|(k, v)| (k.clone(), InternalDbtNode::serialize(v)))
            .collect();
        map.insert(
            "saved_queries".to_string(),
            serde_json::to_value(saved_queries_serialized).map_err(serde::ser::Error::custom)?,
        );

        // Serialize exposures using InternalDbtNode trait
        let exposures_serialized: BTreeMap<String, serde_json::Value> = self
            .exposures
            .iter()
            .map(|(k, v)| (k.clone(), InternalDbtNode::serialize(v)))
            .collect();
        map.insert(
            "exposures".to_string(),
            serde_json::to_value(exposures_serialized).map_err(serde::ser::Error::custom)?,
        );

        // Serialize metrics using InternalDbtNode trait
        let metrics_serialized: BTreeMap<String, serde_json::Value> = self
            .metrics
            .iter()
            .map(|(k, v)| (k.clone(), InternalDbtNode::serialize(v)))
            .collect();
        map.insert(
            "metrics".to_string(),
            serde_json::to_value(metrics_serialized).map_err(serde::ser::Error::custom)?,
        );

        map.insert(
            "child_map".to_string(),
            serde_json::to_value(&self.child_map).map_err(serde::ser::Error::custom)?,
        );
        map.insert(
            "parent_map".to_string(),
            serde_json::to_value(&self.parent_map).map_err(serde::ser::Error::custom)?,
        );
        map.insert(
            "group_map".to_string(),
            serde_json::to_value(&self.group_map).map_err(serde::ser::Error::custom)?,
        );
        map.insert(
            "disabled".to_string(),
            serde_json::to_value(&self.disabled).map_err(serde::ser::Error::custom)?,
        );
        map.insert(
            "selectors".to_string(),
            serde_json::to_value(&self.selectors).map_err(serde::ser::Error::custom)?,
        );
        map.insert(
            "groups".to_string(),
            serde_json::to_value(&self.groups).map_err(serde::ser::Error::custom)?,
        );

        map.serialize(serializer)
    }
}

pub fn build_manifest(invocation_id: &str, resolver_state: &ResolverState) -> DbtManifest {
    DbtManifest {
        metadata: ManifestMetadata {
            base: BaseMetadata {
                dbt_schema_version: get_dbt_schema_version("manifest", 20),
                dbt_version: env!("CARGO_PKG_VERSION").to_string(),
                generated_at: Utc::now(),
                invocation_id: Some(invocation_id.to_string()),
                ..Default::default()
            },
            project_name: resolver_state.root_project_name.clone(),
            adapter_type: resolver_state.dbt_profile.db_config.adapter_type(),
            ..Default::default()
        },
        nodes: resolver_state
            .nodes
            .models
            .iter()
            .map(|(id, node)| (id.clone(), DbtNode::Model((**node).clone())))
            .chain(
                resolver_state
                    .nodes
                    .tests
                    .iter()
                    .map(|(id, node)| (id.clone(), DbtNode::Test((**node).clone()))),
            )
            .chain(
                resolver_state
                    .nodes
                    .snapshots
                    .iter()
                    .map(|(id, node)| (id.clone(), DbtNode::Snapshot((**node).clone()))),
            )
            .chain(
                resolver_state
                    .nodes
                    .seeds
                    .iter()
                    .map(|(id, node)| (id.clone(), DbtNode::Seed((**node).clone()))),
            )
            .chain(
                resolver_state
                    .nodes
                    .tests
                    .iter()
                    .map(|(id, node)| (id.clone(), DbtNode::Test((**node).clone()))),
            )
            .chain(resolver_state.operations.on_run_start.iter().map(|node| {
                (
                    node.common_attr.unique_id.clone(),
                    DbtNode::Operation(node.clone()),
                )
            }))
            .chain(resolver_state.operations.on_run_end.iter().map(|node| {
                (
                    node.common_attr.unique_id.clone(),
                    DbtNode::Operation(node.clone()),
                )
            }))
            .collect(),
        sources: resolver_state
            .nodes
            .sources
            .iter()
            .map(|(id, source)| (id.clone(), (**source).clone()))
            .collect(),
        unit_tests: resolver_state
            .nodes
            .unit_tests
            .iter()
            .map(|(id, unit_test)| (id.clone(), (**unit_test).clone()))
            .collect(),
        macros: resolver_state.macros.macros.clone(),
        docs: resolver_state.macros.docs_macros.clone(),
        ..Default::default()
    }
}
