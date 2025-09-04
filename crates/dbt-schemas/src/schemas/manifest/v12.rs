use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;

use crate::schemas::{
    InternalDbtNode,
    macros::{DbtDocsMacro, DbtMacro},
    manifest::{
        DbtNode, ManifestMetadata,
        manifest::serialize_with_resource_type,
        manifest_nodes::{
            ManifestExposure, ManifestMetric, ManifestSavedQuery, ManifestSemanticModel,
            ManifestSource, ManifestUnitTest,
        },
    },
};

use super::{DbtSelector, ManifestGroup};

#[derive(Debug, Default, Deserialize, Clone)]
pub struct DbtManifestV12 {
    pub metadata: ManifestMetadata,
    pub nodes: BTreeMap<String, DbtNode>,
    pub sources: BTreeMap<String, ManifestSource>,
    pub macros: BTreeMap<String, DbtMacro>,
    pub unit_tests: BTreeMap<String, ManifestUnitTest>,
    pub docs: BTreeMap<String, DbtDocsMacro>,
    pub semantic_models: BTreeMap<String, ManifestSemanticModel>,
    pub saved_queries: BTreeMap<String, ManifestSavedQuery>,
    pub exposures: BTreeMap<String, ManifestExposure>,
    pub metrics: BTreeMap<String, ManifestMetric>,
    pub child_map: BTreeMap<String, Vec<String>>,
    pub parent_map: BTreeMap<String, Vec<String>>,
    pub group_map: BTreeMap<String, Vec<String>>,
    pub disabled: BTreeMap<String, Vec<YmlValue>>,
    pub selectors: BTreeMap<String, DbtSelector>,
    pub groups: BTreeMap<String, ManifestGroup>,
}

impl DbtManifestV12 {
    pub fn into_map_compiled_sql(self) -> HashMap<String, Option<String>> {
        self.nodes
            .into_iter()
            .filter_map(|(id, node)| match node {
                DbtNode::Model(model) => Some((id, model.__base_attr__.compiled_code)),
                DbtNode::Test(test) => Some((id, test.__base_attr__.compiled_code)),
                DbtNode::Snapshot(snapshot) => Some((id, snapshot.__base_attr__.compiled_code)),
                DbtNode::Seed(seed) => Some((id, seed.__base_attr__.compiled_code)),
                DbtNode::Operation(_operation) => None,
                DbtNode::Analysis(analysis) => Some((id, analysis.__base_attr__.compiled_code)),
            })
            .collect::<HashMap<_, _>>()
    }
}

impl Serialize for DbtManifestV12 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map: BTreeMap<String, YmlValue> = BTreeMap::new();
        map.insert(
            "metadata".to_string(),
            dbt_serde_yaml::to_value(&self.metadata).map_err(serde::ser::Error::custom)?,
        );
        map.insert(
            "nodes".to_string(),
            dbt_serde_yaml::to_value(&self.nodes).map_err(serde::ser::Error::custom)?,
        );

        // Serialize sources using InternalDbtNode trait
        let sources_serialized: BTreeMap<String, YmlValue> = self
            .sources
            .iter()
            .map(|(k, v)| {
                Ok((
                    k.clone(),
                    serialize_with_resource_type(
                        dbt_serde_yaml::to_value(v).map_err(serde::ser::Error::custom)?,
                        "source",
                    ),
                ))
            })
            .collect::<Result<_, _>>()?;
        map.insert(
            "sources".to_string(),
            dbt_serde_yaml::to_value(sources_serialized).map_err(serde::ser::Error::custom)?,
        );

        // Serialize macros using InternalDbtNode trait
        let macros_serialized: BTreeMap<String, YmlValue> = self
            .macros
            .iter()
            .map(|(k, v)| (k.clone(), InternalDbtNode::serialize(v)))
            .collect();
        map.insert(
            "macros".to_string(),
            dbt_serde_yaml::to_value(macros_serialized).map_err(serde::ser::Error::custom)?,
        );

        // Serialize unit_tests using InternalDbtNode trait
        let unit_tests_serialized: BTreeMap<String, YmlValue> = self
            .unit_tests
            .iter()
            .map(|(k, v)| {
                Ok((
                    k.clone(),
                    serialize_with_resource_type(
                        dbt_serde_yaml::to_value(v).map_err(serde::ser::Error::custom)?,
                        "unit_test",
                    ),
                ))
            })
            .collect::<Result<_, _>>()?;
        map.insert(
            "unit_tests".to_string(),
            dbt_serde_yaml::to_value(unit_tests_serialized).map_err(serde::ser::Error::custom)?,
        );

        map.insert(
            "docs".to_string(),
            dbt_serde_yaml::to_value(&self.docs).map_err(serde::ser::Error::custom)?,
        );

        // Serialize semantic_models
        let semantic_models_serialized: BTreeMap<String, YmlValue> = self
            .semantic_models
            .iter()
            .map(|(k, v)| {
                Ok((
                    k.clone(),
                    serialize_with_resource_type(
                        dbt_serde_yaml::to_value(v).map_err(serde::ser::Error::custom)?,
                        "semantic_model",
                    ),
                ))
            })
            .collect::<Result<_, _>>()?;
        map.insert(
            "semantic_models".to_string(),
            dbt_serde_yaml::to_value(semantic_models_serialized)
                .map_err(serde::ser::Error::custom)?,
        );

        // Serialize saved queries
        let saved_queries_serialized: BTreeMap<String, YmlValue> = self
            .saved_queries
            .iter()
            .map(|(k, v)| {
                Ok((
                    k.clone(),
                    serialize_with_resource_type(
                        dbt_serde_yaml::to_value(v).map_err(serde::ser::Error::custom)?,
                        "saved_query",
                    ),
                ))
            })
            .collect::<Result<_, _>>()?;
        map.insert(
            "saved_queries".to_string(),
            dbt_serde_yaml::to_value(saved_queries_serialized)
                .map_err(serde::ser::Error::custom)?,
        );

        // Serialize exposures
        let exposures_serialized: BTreeMap<String, YmlValue> = self
            .exposures
            .iter()
            .map(|(k, v)| {
                Ok((
                    k.clone(),
                    serialize_with_resource_type(
                        dbt_serde_yaml::to_value(v).map_err(serde::ser::Error::custom)?,
                        "exposure",
                    ),
                ))
            })
            .collect::<Result<_, _>>()?;
        map.insert(
            "exposures".to_string(),
            dbt_serde_yaml::to_value(exposures_serialized).map_err(serde::ser::Error::custom)?,
        );

        // Serialize metrics
        let metrics_serialized: BTreeMap<String, YmlValue> = self
            .metrics
            .iter()
            .map(|(k, v)| {
                Ok((
                    k.clone(),
                    serialize_with_resource_type(
                        dbt_serde_yaml::to_value(v).map_err(serde::ser::Error::custom)?,
                        "metric",
                    ),
                ))
            })
            .collect::<Result<_, _>>()?;
        map.insert(
            "metrics".to_string(),
            dbt_serde_yaml::to_value(metrics_serialized).map_err(serde::ser::Error::custom)?,
        );

        map.insert(
            "child_map".to_string(),
            dbt_serde_yaml::to_value(&self.child_map).map_err(serde::ser::Error::custom)?,
        );
        map.insert(
            "parent_map".to_string(),
            dbt_serde_yaml::to_value(&self.parent_map).map_err(serde::ser::Error::custom)?,
        );
        map.insert(
            "group_map".to_string(),
            dbt_serde_yaml::to_value(&self.group_map).map_err(serde::ser::Error::custom)?,
        );
        map.insert(
            "disabled".to_string(),
            dbt_serde_yaml::to_value(&self.disabled).map_err(serde::ser::Error::custom)?,
        );
        map.insert(
            "selectors".to_string(),
            dbt_serde_yaml::to_value(&self.selectors).map_err(serde::ser::Error::custom)?,
        );
        map.insert(
            "groups".to_string(),
            dbt_serde_yaml::to_value(&self.groups).map_err(serde::ser::Error::custom)?,
        );

        map.serialize(serializer)
    }
}
