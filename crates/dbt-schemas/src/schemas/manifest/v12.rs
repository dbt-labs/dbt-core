use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

use crate::schemas::{
    InternalDbtNode,
    macros::{DbtDocsMacro, DbtMacro},
    manifest::{
        DbtNode, ManifestMetadata,
        manifest::serialize_with_resource_type,
        manifest_nodes::{ManifestSource, ManifestUnitTest},
    },
};

use super::{DbtExposure, DbtGroup, DbtMetric, DbtSavedQuery, DbtSelector, DbtSemanticModel};

#[derive(Debug, Default, Deserialize)]
pub struct DbtManifestV12 {
    pub metadata: ManifestMetadata,
    pub nodes: BTreeMap<String, DbtNode>,
    pub sources: BTreeMap<String, ManifestSource>,
    pub macros: BTreeMap<String, DbtMacro>,
    pub unit_tests: BTreeMap<String, ManifestUnitTest>,
    pub docs: BTreeMap<String, DbtDocsMacro>,
    pub semantic_models: BTreeMap<String, DbtSemanticModel>,
    pub saved_queries: BTreeMap<String, DbtSavedQuery>,
    pub exposures: BTreeMap<String, DbtExposure>,
    pub metrics: BTreeMap<String, DbtMetric>,
    pub child_map: BTreeMap<String, Vec<String>>,
    pub parent_map: BTreeMap<String, Vec<String>>,
    pub group_map: BTreeMap<String, Vec<String>>,
    pub disabled: BTreeMap<String, Vec<Value>>,
    pub selectors: BTreeMap<String, DbtSelector>,
    pub groups: BTreeMap<String, DbtGroup>,
}

impl DbtManifestV12 {
    pub fn into_map_compiled_sql(self) -> HashMap<String, Option<String>> {
        self.nodes
            .into_iter()
            .filter_map(|(id, node)| match node {
                DbtNode::Model(model) => Some((id, model.base_attr.compiled_code)),
                DbtNode::Test(test) => Some((id, test.base_attr.compiled_code)),
                DbtNode::Snapshot(snapshot) => Some((id, snapshot.base_attr.compiled_code)),
                DbtNode::Seed(seed) => Some((id, seed.base_attr.compiled_code)),
                DbtNode::Operation(_operation) => None,
                DbtNode::Analysis(analysis) => Some((id, analysis.base_attr.compiled_code)),
            })
            .collect::<HashMap<_, _>>()
    }
}

impl Serialize for DbtManifestV12 {
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
        let sources_serialized: BTreeMap<String, Value> = self
            .sources
            .iter()
            .map(|(k, v)| {
                Ok((
                    k.clone(),
                    serialize_with_resource_type(
                        serde_json::to_value(v).map_err(serde::ser::Error::custom)?,
                        "source",
                    ),
                ))
            })
            .collect::<Result<_, _>>()?;
        map.insert(
            "sources".to_string(),
            serde_json::to_value(sources_serialized).map_err(serde::ser::Error::custom)?,
        );

        // Serialize macros using InternalDbtNode trait
        let macros_serialized: BTreeMap<String, Value> = self
            .macros
            .iter()
            .map(|(k, v)| (k.clone(), InternalDbtNode::serialize(v)))
            .collect();
        map.insert(
            "macros".to_string(),
            serde_json::to_value(macros_serialized).map_err(serde::ser::Error::custom)?,
        );

        // Serialize unit_tests using InternalDbtNode trait
        let unit_tests_serialized: BTreeMap<String, Value> = self
            .unit_tests
            .iter()
            .map(|(k, v)| {
                Ok((
                    k.clone(),
                    serialize_with_resource_type(
                        serde_json::to_value(v).map_err(serde::ser::Error::custom)?,
                        "unit_test",
                    ),
                ))
            })
            .collect::<Result<_, _>>()?;
        map.insert(
            "unit_tests".to_string(),
            serde_json::to_value(unit_tests_serialized).map_err(serde::ser::Error::custom)?,
        );

        map.insert(
            "docs".to_string(),
            serde_json::to_value(&self.docs).map_err(serde::ser::Error::custom)?,
        );

        // Serialize semantic_models using InternalDbtNode trait
        let semantic_models_serialized: BTreeMap<String, Value> = self
            .semantic_models
            .iter()
            .map(|(k, v)| (k.clone(), InternalDbtNode::serialize(v)))
            .collect();
        map.insert(
            "semantic_models".to_string(),
            serde_json::to_value(semantic_models_serialized).map_err(serde::ser::Error::custom)?,
        );

        // Serialize saved_queries using InternalDbtNode trait
        let saved_queries_serialized: BTreeMap<String, Value> = self
            .saved_queries
            .iter()
            .map(|(k, v)| (k.clone(), InternalDbtNode::serialize(v)))
            .collect();
        map.insert(
            "saved_queries".to_string(),
            serde_json::to_value(saved_queries_serialized).map_err(serde::ser::Error::custom)?,
        );

        // Serialize exposures using InternalDbtNode trait
        let exposures_serialized: BTreeMap<String, Value> = self
            .exposures
            .iter()
            .map(|(k, v)| (k.clone(), InternalDbtNode::serialize(v)))
            .collect();
        map.insert(
            "exposures".to_string(),
            serde_json::to_value(exposures_serialized).map_err(serde::ser::Error::custom)?,
        );

        // Serialize metrics using InternalDbtNode trait
        let metrics_serialized: BTreeMap<String, Value> = self
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
