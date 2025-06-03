use core::fmt;
use std::{any::Any, collections::BTreeMap, fmt::Display, path::PathBuf, sync::Arc};

use dbt_common::{err, io_args::StaticAnalysisKind, ErrorCode, FsResult};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use serde_with::skip_serializing_none;

use crate::schemas::{
    common::{
        Access, Constraint, DatabricksModelConfig, DbtBatchSize, DbtChecksum, DbtContract,
        DbtIncrementalStrategy, DbtMaterialization, DbtQuoting, DbtUniqueKey, DocsConfig, Expect,
        FreshnessDefinition, Given, Hooks, IncludeExclude, NodeDependsOn, OnConfigurationChange,
        OnSchemaChange, PersistDocsConfig, SnowflakeModelConfig,
    },
    dbt_column::DbtColumn,
    macros::DbtMacro,
    manifest::DbtConfig,
    properties::ModelFreshness,
    ref_and_source::{DbtRef, DbtSourceWrapper},
    serde::{default_type, StringOrInteger},
};

use super::{
    manifest::DbtNode, BigQueryModelConfig, DbtExposure, DbtManifest, DbtMetric, DbtSavedQuery,
    DbtSemanticModel,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub enum IntrospectionKind {
    Execute,
    UpstreamSchema,
    InternalSchema,
    ExternalSchema,
    Unknown,
}

impl Display for IntrospectionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IntrospectionKind::Execute => write!(f, "execute"),
            IntrospectionKind::UpstreamSchema => write!(f, "upstream_schema"),
            IntrospectionKind::InternalSchema => write!(f, "internal_schema"),
            IntrospectionKind::ExternalSchema => write!(f, "external_schema"),
            IntrospectionKind::Unknown => write!(f, "unknown"),
        }
    }
}

pub trait InternalDbtNode: Any + Send + Sync + fmt::Debug {
    fn common(&self) -> &CommonAttributes;
    fn base(&self) -> NodeBaseAttributes;
    fn base_mut(&mut self) -> Option<&mut NodeBaseAttributes>;
    fn common_mut(&mut self) -> &mut CommonAttributes;
    fn get_dbt_config(&self) -> DbtConfig;
    fn set_dbt_config(&mut self, config: DbtConfig);
    fn version(&self) -> Option<StringOrInteger> {
        None
    }
    fn latest_version(&self) -> Option<StringOrInteger> {
        None
    }
    fn is_extended_model(&self) -> bool {
        false
    }
    fn is_versioned(&self) -> bool {
        false
    }
    fn resource_type(&self) -> &str;
    fn as_any(&self) -> &dyn Any;
    fn serialize(&self) -> Value {
        let mut ret = self.serialize_inner();
        if let Value::Object(ref mut map) = ret {
            map.insert(
                "resource_type".to_string(),
                Value::String(self.resource_type().to_string()),
            );
        }
        ret
    }
    fn serialize_inner(&self) -> Value;

    // Selector functions
    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool;
    fn has_same_content(&self, other: &dyn InternalDbtNode) -> bool;
    fn set_detected_introspection(&mut self, introspection: Option<IntrospectionKind>);
    fn introspection(&self) -> Option<IntrospectionKind> {
        None
    }
    fn get_static_analysis(&self) -> Option<StaticAnalysisKind> {
        None
    }

    fn is_test(&self) -> bool {
        self.resource_type() == "test"
    }

    // Incremental strategy validation
    fn warn_on_microbatch(&self) -> FsResult<()> {
        let config = self.get_dbt_config();
        if let Some(DbtIncrementalStrategy::Microbatch) = config.incremental_strategy {
            return err!(
                code => ErrorCode::UnsupportedFeature,
                loc => self.common().path.clone(),
                "Microbatch incremental strategy is not supported. Use --exclude config.incremental_strategy:microbatch to exclude these models."
            );
        }
        Ok(())
    }
    fn get_node_start_data(&self) -> Value {
        json!({
            "node_info":{
                "node_name": self.common().name,
                "unique_id": self.common().unique_id,
                "node_started_at": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f").to_string(),
                "node_status": "executing"
            }
        })
    }

    fn get_node_end_data(&self, status: &str) -> Value {
        json!({
            "completed_at": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f").to_string(),
            "node_info":{
                "node_name": self.common().name,
                "unique_id": self.common().unique_id,
                "node_finished_at": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f").to_string(),
                "node_status": status
            },
        })
    }
}

impl InternalDbtNode for DbtModel {
    fn common(&self) -> &CommonAttributes {
        &self.common_attr
    }
    fn base(&self) -> NodeBaseAttributes {
        self.base_attr.clone()
    }
    fn get_dbt_config(&self) -> DbtConfig {
        self.config.clone().into()
    }
    fn set_dbt_config(&mut self, config: DbtConfig) {
        self.config = ManifestModelConfig::from(config);
    }
    fn version(&self) -> Option<StringOrInteger> {
        self.version.clone()
    }
    fn latest_version(&self) -> Option<StringOrInteger> {
        self.latest_version.clone()
    }
    fn is_versioned(&self) -> bool {
        self.version.is_some()
    }
    fn is_extended_model(&self) -> bool {
        self.is_extended_model
    }
    fn resource_type(&self) -> &str {
        "model"
    }
    fn base_mut(&mut self) -> Option<&mut NodeBaseAttributes> {
        Some(&mut self.base_attr)
    }
    fn common_mut(&mut self) -> &mut CommonAttributes {
        &mut self.common_attr
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn serialize_inner(&self) -> Value {
        serde_json::to_value(self).expect("Failed to serialize DbtModel")
    }
    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_model) = other.as_any().downcast_ref::<DbtModel>() {
            self.config == other_model.config
        } else {
            false
        }
    }
    fn has_same_content(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_model) = other.as_any().downcast_ref::<DbtModel>() {
            self.base_attr.checksum == other_model.base_attr.checksum
        } else {
            false
        }
    }
    fn set_detected_introspection(&mut self, introspection: Option<IntrospectionKind>) {
        self.introspection = introspection;
    }
    fn introspection(&self) -> Option<IntrospectionKind> {
        self.introspection
    }
    fn get_static_analysis(&self) -> Option<StaticAnalysisKind> {
        self.config.static_analysis
    }
}

impl InternalDbtNode for DbtSeed {
    fn resource_type(&self) -> &str {
        "seed"
    }
    fn common(&self) -> &CommonAttributes {
        &self.common_attr
    }
    fn common_mut(&mut self) -> &mut CommonAttributes {
        &mut self.common_attr
    }
    fn base(&self) -> NodeBaseAttributes {
        self.base_attr.clone()
    }
    fn base_mut(&mut self) -> Option<&mut NodeBaseAttributes> {
        Some(&mut self.base_attr)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_dbt_config(&self) -> DbtConfig {
        self.config.clone()
    }
    fn set_dbt_config(&mut self, config: DbtConfig) {
        self.config = config;
    }
    fn serialize_inner(&self) -> Value {
        serde_json::to_value(self).expect("Failed to serialize DbtSeed")
    }
    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_model) = other.as_any().downcast_ref::<DbtSeed>() {
            self.config == other_model.config
        } else {
            false
        }
    }
    fn has_same_content(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_model) = other.as_any().downcast_ref::<DbtSeed>() {
            self.base_attr.checksum == other_model.base_attr.checksum
        } else {
            false
        }
    }
    fn set_detected_introspection(&mut self, _introspection: Option<IntrospectionKind>) {
        panic!("DbtSeed does not support setting detected_unsafe");
    }
}

impl InternalDbtNode for DbtTest {
    fn common(&self) -> &CommonAttributes {
        &self.common_attr
    }
    fn base(&self) -> NodeBaseAttributes {
        self.base_attr.clone()
    }
    fn get_dbt_config(&self) -> DbtConfig {
        self.config.clone()
    }
    fn set_dbt_config(&mut self, config: DbtConfig) {
        self.config = config;
    }
    fn resource_type(&self) -> &str {
        "test"
    }
    fn base_mut(&mut self) -> Option<&mut NodeBaseAttributes> {
        Some(&mut self.base_attr)
    }
    fn common_mut(&mut self) -> &mut CommonAttributes {
        &mut self.common_attr
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn serialize_inner(&self) -> Value {
        serde_json::to_value(self).expect("Failed to serialize DbtTest")
    }
    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other) = other.as_any().downcast_ref::<DbtTest>() {
            // these fields are what dbt compares for test nodes
            // Some other configs were skipped
            self.config.enabled == other.config.enabled
                && self.config.alias == other.config.alias
                && self.config.database == other.config.database
                && self.config.tags == other.config.tags
                && self.config.meta == other.config.meta
                && self.config.group == other.config.group
                && self.config.materialized == other.config.materialized
                && self.config.incremental_strategy == other.config.incremental_strategy
                && self.config.persist_docs == other.config.persist_docs
                && self.config.post_hook == other.config.post_hook
                && self.config.pre_hook == other.config.pre_hook
                && self.config.quoting == other.config.quoting
                && self.config.column_types == other.config.column_types
                && self.config.full_refresh == other.config.full_refresh
                && self.config.unique_key == other.config.unique_key
                && self.config.on_schema_change == other.config.on_schema_change
                && self.config.on_configuration_change == other.config.on_configuration_change
                && self.config.grants == other.config.grants
                && self.config.packages == other.config.packages
                && self.config.docs == other.config.docs
                && self.config.access == other.config.access
        } else {
            false
        }
    }
    fn has_same_content(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other) = other.as_any().downcast_ref::<DbtTest>() {
            self.common().fqn == other.common().fqn
        } else {
            false
        }
    }
    fn set_detected_introspection(&mut self, _introspection: Option<IntrospectionKind>) {
        panic!("DbtTest does not support setting detected_unsafe");
    }
}

impl InternalDbtNode for DbtUnitTest {
    fn common(&self) -> &CommonAttributes {
        &self.common_attr
    }
    fn base(&self) -> NodeBaseAttributes {
        self.base_attr.clone()
    }
    fn get_dbt_config(&self) -> DbtConfig {
        self.config.clone()
    }
    fn set_dbt_config(&mut self, config: DbtConfig) {
        self.config = config;
    }
    fn resource_type(&self) -> &str {
        "unit_test"
    }
    fn base_mut(&mut self) -> Option<&mut NodeBaseAttributes> {
        Some(&mut self.base_attr)
    }
    fn common_mut(&mut self) -> &mut CommonAttributes {
        &mut self.common_attr
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn serialize_inner(&self) -> Value {
        serde_json::to_value(self).expect("Failed to serialize DbtUnitTest")
    }
    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other) = other.as_any().downcast_ref::<DbtUnitTest>() {
            self.config == other.config
        } else {
            false
        }
    }
    fn has_same_content(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other) = other.as_any().downcast_ref::<DbtUnitTest>() {
            self.common().fqn == other.common().fqn
        } else {
            false
        }
    }
    fn set_detected_introspection(&mut self, _introspection: Option<IntrospectionKind>) {
        panic!("DbtUnitTest does not support setting detected_unsafe");
    }
}

impl InternalDbtNode for DbtSource {
    fn common(&self) -> &CommonAttributes {
        &self.common_attr
    }
    fn base(&self) -> NodeBaseAttributes {
        self.get_base_attr()
    }
    fn get_dbt_config(&self) -> DbtConfig {
        self.config.clone()
    }
    fn set_dbt_config(&mut self, config: DbtConfig) {
        self.config = config;
    }
    fn resource_type(&self) -> &str {
        "source"
    }
    fn base_mut(&mut self) -> Option<&mut NodeBaseAttributes> {
        None
    }
    fn common_mut(&mut self) -> &mut CommonAttributes {
        &mut self.common_attr
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn serialize_inner(&self) -> Value {
        serde_json::to_value(self).expect("Failed to serialize DbtSource")
    }
    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_source) = other.as_any().downcast_ref::<DbtSource>() {
            self.config == other_source.config
        } else {
            false
        }
    }
    fn has_same_content(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_source) = other.as_any().downcast_ref::<DbtSource>() {
            self.common_attr.database == other_source.common_attr.database
                && self.common_attr.schema == other_source.common_attr.schema
                && self.common_attr.name == other_source.common_attr.name
                && self.identifier == other_source.identifier
                && self.common_attr.fqn == other_source.common_attr.fqn
                && self.config == other_source.config
                && self.quoting == other_source.quoting
                && self.loaded_at_field == other_source.loaded_at_field
                && self.loader == other_source.loader
        } else {
            false
        }
    }
    fn set_detected_introspection(&mut self, _introspection: Option<IntrospectionKind>) {
        panic!("DbtSource does not support setting detected_unsafe");
    }
}

impl InternalDbtNode for DbtSnapshot {
    fn common(&self) -> &CommonAttributes {
        &self.common_attr
    }
    fn base(&self) -> NodeBaseAttributes {
        self.base_attr.clone()
    }
    fn get_dbt_config(&self) -> DbtConfig {
        self.config.clone()
    }
    fn set_dbt_config(&mut self, config: DbtConfig) {
        self.config = config;
    }
    fn resource_type(&self) -> &str {
        "snapshot"
    }
    fn base_mut(&mut self) -> Option<&mut NodeBaseAttributes> {
        Some(&mut self.base_attr)
    }
    fn common_mut(&mut self) -> &mut CommonAttributes {
        &mut self.common_attr
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn serialize_inner(&self) -> Value {
        serde_json::to_value(self).expect("Failed to serialize DbtSnapshot")
    }
    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_snapshot) = other.as_any().downcast_ref::<DbtSnapshot>() {
            self.config == other_snapshot.config
        } else {
            false
        }
    }
    fn has_same_content(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_snapshot) = other.as_any().downcast_ref::<DbtSnapshot>() {
            self.base_attr.checksum == other_snapshot.base_attr.checksum
        } else {
            false
        }
    }
    fn set_detected_introspection(&mut self, _introspection: Option<IntrospectionKind>) {
        panic!("DbtSnapshot does not support setting detected_unsafe");
    }
}

impl InternalDbtNode for DbtSemanticModel {
    fn common(&self) -> &CommonAttributes {
        unimplemented!()
    }
    fn base(&self) -> NodeBaseAttributes {
        unimplemented!()
    }
    fn base_mut(&mut self) -> Option<&mut NodeBaseAttributes> {
        unimplemented!()
    }
    fn common_mut(&mut self) -> &mut CommonAttributes {
        unimplemented!()
    }
    fn get_dbt_config(&self) -> DbtConfig {
        self.config.clone()
    }
    fn set_dbt_config(&mut self, config: DbtConfig) {
        self.config = config;
    }
    fn resource_type(&self) -> &str {
        "semantic_model"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn serialize_inner(&self) -> Value {
        serde_json::to_value(self).expect("Failed to serialize DbtSnapshot")
    }
    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_semantic_model) = other.as_any().downcast_ref::<DbtSemanticModel>() {
            self.config == other_semantic_model.config
        } else {
            false
        }
    }
    fn has_same_content(&self, _other: &dyn InternalDbtNode) -> bool {
        unimplemented!()
    }
    fn set_detected_introspection(&mut self, _introspection: Option<IntrospectionKind>) {
        panic!("DbtSemanticModel does not support setting detected_unsafe");
    }
}

impl InternalDbtNode for DbtExposure {
    fn common(&self) -> &CommonAttributes {
        unimplemented!()
    }
    fn base(&self) -> NodeBaseAttributes {
        unimplemented!()
    }
    fn base_mut(&mut self) -> Option<&mut NodeBaseAttributes> {
        unimplemented!()
    }
    fn common_mut(&mut self) -> &mut CommonAttributes {
        unimplemented!()
    }
    fn get_dbt_config(&self) -> DbtConfig {
        self.config.clone()
    }
    fn set_dbt_config(&mut self, config: DbtConfig) {
        self.config = config;
    }
    fn resource_type(&self) -> &str {
        "exposure"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn serialize_inner(&self) -> Value {
        serde_json::to_value(self).expect("Failed to serialize DbtExposure")
    }
    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_exposure) = other.as_any().downcast_ref::<DbtExposure>() {
            self.config == other_exposure.config
        } else {
            false
        }
    }

    fn has_same_content(&self, _other: &dyn InternalDbtNode) -> bool {
        unimplemented!()
    }
    fn set_detected_introspection(&mut self, _introspection: Option<IntrospectionKind>) {
        panic!("DbtExposure does not support setting detected_unsafe");
    }
}

impl InternalDbtNode for DbtSavedQuery {
    fn common(&self) -> &CommonAttributes {
        unimplemented!()
    }
    fn base(&self) -> NodeBaseAttributes {
        unimplemented!()
    }
    fn base_mut(&mut self) -> Option<&mut NodeBaseAttributes> {
        unimplemented!()
    }
    fn common_mut(&mut self) -> &mut CommonAttributes {
        unimplemented!()
    }
    fn get_dbt_config(&self) -> DbtConfig {
        self.config.clone()
    }
    fn set_dbt_config(&mut self, config: DbtConfig) {
        self.config = config;
    }
    fn resource_type(&self) -> &str {
        "saved_query"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn serialize_inner(&self) -> Value {
        serde_json::to_value(self).expect("Failed to serialize DbtSavedQuery")
    }
    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_saved_query) = other.as_any().downcast_ref::<DbtSavedQuery>() {
            self.config == other_saved_query.config
        } else {
            false
        }
    }
    fn has_same_content(&self, _other: &dyn InternalDbtNode) -> bool {
        unimplemented!()
    }
    fn set_detected_introspection(&mut self, _introspection: Option<IntrospectionKind>) {
        panic!("DbtSavedQuery does not support setting detected_unsafe");
    }
}

impl InternalDbtNode for DbtMetric {
    fn common(&self) -> &CommonAttributes {
        unimplemented!()
    }
    fn base(&self) -> NodeBaseAttributes {
        unimplemented!()
    }
    fn base_mut(&mut self) -> Option<&mut NodeBaseAttributes> {
        unimplemented!()
    }
    fn common_mut(&mut self) -> &mut CommonAttributes {
        unimplemented!()
    }
    fn get_dbt_config(&self) -> DbtConfig {
        self.config.clone()
    }
    fn set_dbt_config(&mut self, config: DbtConfig) {
        self.config = config;
    }
    fn resource_type(&self) -> &str {
        "metric"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn serialize_inner(&self) -> Value {
        serde_json::to_value(self).expect("Failed to serialize DbtMetric")
    }
    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_metric) = other.as_any().downcast_ref::<DbtMetric>() {
            self.config == other_metric.config
        } else {
            false
        }
    }
    fn has_same_content(&self, _other: &dyn InternalDbtNode) -> bool {
        unimplemented!()
    }
    fn set_detected_introspection(&mut self, _introspection: Option<IntrospectionKind>) {
        panic!("DbtMetric does not support setting detected_unsafe");
    }
}

impl InternalDbtNode for DbtMacro {
    fn common(&self) -> &CommonAttributes {
        unimplemented!()
    }
    fn base(&self) -> NodeBaseAttributes {
        unimplemented!()
    }
    fn base_mut(&mut self) -> Option<&mut NodeBaseAttributes> {
        unimplemented!()
    }
    fn common_mut(&mut self) -> &mut CommonAttributes {
        unimplemented!()
    }
    fn get_dbt_config(&self) -> DbtConfig {
        unimplemented!()
    }
    fn set_dbt_config(&mut self, _config: DbtConfig) {
        unimplemented!()
    }
    fn resource_type(&self) -> &str {
        "macro"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn serialize_inner(&self) -> Value {
        serde_json::to_value(self).expect("Failed to serialize DbtMacro")
    }
    fn has_same_config(&self, _other: &dyn InternalDbtNode) -> bool {
        unimplemented!()
    }
    fn has_same_content(&self, _other: &dyn InternalDbtNode) -> bool {
        unimplemented!()
    }
    fn set_detected_introspection(&mut self, _introspection: Option<IntrospectionKind>) {
        panic!("DbtMacro does not support setting detected_unsafe");
    }
}

#[derive(Debug, Default, Clone)]
pub struct Nodes {
    pub models: BTreeMap<String, Arc<DbtModel>>,
    pub seeds: BTreeMap<String, Arc<DbtSeed>>,
    pub tests: BTreeMap<String, Arc<DbtTest>>,
    pub unit_tests: BTreeMap<String, Arc<DbtUnitTest>>,
    pub sources: BTreeMap<String, Arc<DbtSource>>,
    pub snapshots: BTreeMap<String, Arc<DbtSnapshot>>,
    pub analyses: BTreeMap<String, Arc<DbtModel>>,
}

impl Nodes {
    pub fn deep_clone(&self) -> Self {
        let models = self
            .models
            .iter()
            .map(|(id, node)| (id.clone(), Arc::new((**node).clone())))
            .collect();
        let seeds = self
            .seeds
            .iter()
            .map(|(id, node)| (id.clone(), Arc::new((**node).clone())))
            .collect();
        let tests = self
            .tests
            .iter()
            .map(|(id, node)| (id.clone(), Arc::new((**node).clone())))
            .collect();
        let unit_tests = self
            .unit_tests
            .iter()
            .map(|(id, node)| (id.clone(), Arc::new((**node).clone())))
            .collect();
        let sources = self
            .sources
            .iter()
            .map(|(id, node)| (id.clone(), Arc::new((**node).clone())))
            .collect();
        let snapshots = self
            .snapshots
            .iter()
            .map(|(id, node)| (id.clone(), Arc::new((**node).clone())))
            .collect();
        let analyses = self
            .analyses
            .iter()
            .map(|(id, node)| (id.clone(), Arc::new((**node).clone())))
            .collect();
        Nodes {
            models,
            seeds,
            tests,
            unit_tests,
            sources,
            snapshots,
            analyses,
        }
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.models
            .keys()
            .chain(self.seeds.keys())
            .chain(self.tests.keys())
            .chain(self.unit_tests.keys())
            .chain(self.sources.keys())
            .chain(self.snapshots.keys())
            .chain(self.analyses.keys())
    }

    pub fn get_node(&self, unique_id: &str) -> Option<&dyn InternalDbtNode> {
        self.models
            .get(unique_id)
            .map(|n| Arc::as_ref(n) as &dyn InternalDbtNode)
            .or_else(|| {
                self.seeds
                    .get(unique_id)
                    .map(|n| Arc::as_ref(n) as &dyn InternalDbtNode)
            })
            .or_else(|| {
                self.tests
                    .get(unique_id)
                    .map(|n| Arc::as_ref(n) as &dyn InternalDbtNode)
            })
            .or_else(|| {
                self.unit_tests
                    .get(unique_id)
                    .map(|n| Arc::as_ref(n) as &dyn InternalDbtNode)
            })
            .or_else(|| {
                self.sources
                    .get(unique_id)
                    .map(|n| Arc::as_ref(n) as &dyn InternalDbtNode)
            })
            .or_else(|| {
                self.snapshots
                    .get(unique_id)
                    .map(|n| Arc::as_ref(n) as &dyn InternalDbtNode)
            })
            .or_else(|| {
                self.analyses
                    .get(unique_id)
                    .map(|n| Arc::as_ref(n) as &dyn InternalDbtNode)
            })
    }

    pub fn contains(&self, unique_id: &str) -> bool {
        self.models.contains_key(unique_id)
            || self.seeds.contains_key(unique_id)
            || self.tests.contains_key(unique_id)
            || self.unit_tests.contains_key(unique_id)
            || self.sources.contains_key(unique_id)
            || self.snapshots.contains_key(unique_id)
            || self.analyses.contains_key(unique_id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &dyn InternalDbtNode)> + '_ {
        self.models
            .iter()
            .map(|(id, node)| (id, Arc::as_ref(node) as &dyn InternalDbtNode))
            .chain(
                self.seeds
                    .iter()
                    .map(|(id, node)| (id, Arc::as_ref(node) as &dyn InternalDbtNode)),
            )
            .chain(
                self.tests
                    .iter()
                    .map(|(id, node)| (id, Arc::as_ref(node) as &dyn InternalDbtNode)),
            )
            .chain(
                self.unit_tests
                    .iter()
                    .map(|(id, node)| (id, Arc::as_ref(node) as &dyn InternalDbtNode)),
            )
            .chain(
                self.sources
                    .iter()
                    .map(|(id, node)| (id, Arc::as_ref(node) as &dyn InternalDbtNode)),
            )
            .chain(
                self.snapshots
                    .iter()
                    .map(|(id, node)| (id, Arc::as_ref(node) as &dyn InternalDbtNode)),
            )
            .chain(
                self.analyses
                    .iter()
                    .map(|(id, node)| (id, Arc::as_ref(node) as &dyn InternalDbtNode)),
            )
    }

    pub fn into_iter(&self) -> impl Iterator<Item = (String, Arc<dyn InternalDbtNode>)> + '_ {
        let models = self
            .models
            .iter()
            .map(|(id, node)| (id.clone(), upcast(node.clone())));
        let seeds = self
            .seeds
            .iter()
            .map(|(id, node)| (id.clone(), upcast(node.clone())));
        let tests = self
            .tests
            .iter()
            .map(|(id, node)| (id.clone(), upcast(node.clone())));
        let unit_tests = self
            .unit_tests
            .iter()
            .map(|(id, node)| (id.clone(), upcast(node.clone())));
        let sources = self
            .sources
            .iter()
            .map(|(id, node)| (id.clone(), upcast(node.clone())));
        let snapshots = self
            .snapshots
            .iter()
            .map(|(id, node)| (id.clone(), upcast(node.clone())));
        let analyses = self
            .analyses
            .iter()
            .map(|(id, node)| (id.clone(), upcast(node.clone())));

        models
            .chain(seeds)
            .chain(tests)
            .chain(unit_tests)
            .chain(sources)
            .chain(snapshots)
            .chain(analyses)
    }

    pub fn iter_values_mut(&mut self) -> impl Iterator<Item = &mut dyn InternalDbtNode> + '_ {
        let map_models = self
            .models
            .values_mut()
            .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNode);
        let map_seeds = self
            .seeds
            .values_mut()
            .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNode);
        let map_tests = self
            .tests
            .values_mut()
            .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNode);
        let map_unit_tests = self
            .unit_tests
            .values_mut()
            .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNode);
        let map_sources = self
            .sources
            .values_mut()
            .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNode);
        let map_snapshots = self
            .snapshots
            .values_mut()
            .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNode);
        let map_analyses = self
            .analyses
            .values_mut()
            .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNode);

        map_models
            .chain(map_seeds)
            .chain(map_tests)
            .chain(map_unit_tests)
            .chain(map_sources)
            .chain(map_snapshots)
            .chain(map_analyses)
    }

    pub fn get_value_mut(&mut self, unique_id: &str) -> Option<&mut dyn InternalDbtNode> {
        self.models
            .get_mut(unique_id)
            .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNode)
            .or_else(|| {
                self.seeds
                    .get_mut(unique_id)
                    .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNode)
            })
            .or_else(|| {
                self.tests
                    .get_mut(unique_id)
                    .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNode)
            })
            .or_else(|| {
                self.unit_tests
                    .get_mut(unique_id)
                    .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNode)
            })
            .or_else(|| {
                self.sources
                    .get_mut(unique_id)
                    .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNode)
            })
            .or_else(|| {
                self.snapshots
                    .get_mut(unique_id)
                    .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNode)
            })
            .or_else(|| {
                self.analyses
                    .get_mut(unique_id)
                    .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNode)
            })
    }

    pub fn get_by_relation_name(&self, relation_name: &str) -> Option<&dyn InternalDbtNode> {
        self.models
            .values()
            .find(|model| model.base().relation_name == Some(relation_name.to_string()))
            .map(|arc| Arc::as_ref(arc) as &dyn InternalDbtNode)
            .or_else(|| {
                self.seeds
                    .values()
                    .find(|seed| seed.base().relation_name == Some(relation_name.to_string()))
                    .map(|arc| Arc::as_ref(arc) as &dyn InternalDbtNode)
            })
            .or_else(|| {
                self.tests
                    .values()
                    .find(|test| test.base().relation_name == Some(relation_name.to_string()))
                    .map(|arc| Arc::as_ref(arc) as &dyn InternalDbtNode)
            })
            .or_else(|| {
                self.unit_tests
                    .values()
                    .find(|unit_test| {
                        unit_test.base().relation_name == Some(relation_name.to_string())
                    })
                    .map(|arc| Arc::as_ref(arc) as &dyn InternalDbtNode)
            })
            .or_else(|| {
                self.sources
                    .values()
                    .find(|source| source.base().relation_name == Some(relation_name.to_string()))
                    .map(|arc| Arc::as_ref(arc) as &dyn InternalDbtNode)
            })
            .or_else(|| {
                self.snapshots
                    .values()
                    .find(|snapshot| {
                        snapshot.base().relation_name == Some(relation_name.to_string())
                    })
                    .map(|arc| Arc::as_ref(arc) as &dyn InternalDbtNode)
            })
            .or_else(|| {
                self.analyses
                    .values()
                    .find(|analysis| {
                        analysis.base().relation_name == Some(relation_name.to_string())
                    })
                    .map(|arc| Arc::as_ref(arc) as &dyn InternalDbtNode)
            })
    }

    pub fn extend(&mut self, other: Nodes) {
        self.models.extend(other.models);
        self.seeds.extend(other.seeds);
        self.tests.extend(other.tests);
        self.unit_tests.extend(other.unit_tests);
        self.sources.extend(other.sources);
        self.snapshots.extend(other.snapshots);
        self.analyses.extend(other.analyses);
    }

    pub fn warn_on_custom_materializations(&self) -> FsResult<()> {
        let mut custom_materializations: Vec<(String, String)> = Vec::new();

        for (_, node) in self.iter() {
            let config = node.get_dbt_config();
            if let Some(DbtMaterialization::Unknown(custom)) = config.materialized {
                custom_materializations.push((node.common().unique_id.clone(), custom));
            }
        }

        if !custom_materializations.is_empty() {
            let mut message = "Custom materialization macros are not supported. Found custom materializations in the following nodes:\n".to_string();
            for (unique_id, materialization) in &custom_materializations {
                message.push_str(&format!(
                    "  - {} (materialization: {})\n",
                    unique_id, materialization
                ));
            }

            return err!(ErrorCode::UnsupportedFeature, "{}", message);
        }
        Ok(())
    }

    pub fn warn_on_microbatch(&self) -> FsResult<()> {
        for (_, node) in self.iter() {
            node.warn_on_microbatch()?;
        }
        Ok(())
    }
}

fn upcast<T: InternalDbtNode + 'static>(arc: Arc<T>) -> Arc<dyn InternalDbtNode> {
    arc
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct CommonAttributes {
    // Identifiers
    pub unique_id: String,
    #[serde(default)]
    pub database: String,
    pub schema: String,
    pub name: String,
    pub package_name: String,
    pub fqn: Vec<String>,

    // Paths
    pub path: PathBuf,
    pub original_file_path: PathBuf,
    pub patch_path: Option<PathBuf>,

    // Meta
    pub description: Option<String>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct NodeBaseAttributes {
    // Identifiers
    #[serde(default)]
    pub alias: String,
    pub relation_name: Option<String>,

    // Paths
    pub compiled_path: Option<String>,
    pub build_path: Option<String>,

    // Derived
    #[serde(default)]
    pub columns: BTreeMap<String, DbtColumn>,
    pub depends_on: NodeDependsOn,
    #[serde(default)]
    pub refs: Vec<DbtRef>,
    #[serde(default)]
    pub sources: Vec<DbtSourceWrapper>,

    // Code
    pub raw_code: Option<String>,
    pub compiled: Option<bool>,
    pub compiled_code: Option<String>,
    #[serde(default)]
    pub unrendered_config: BTreeMap<String, Value>,

    // Metadata
    pub doc_blocks: Option<Vec<Value>>,
    pub extra_ctes_injected: Option<bool>,
    pub extra_ctes: Option<Vec<Value>>,
    #[serde(default)]
    pub metrics: Vec<Vec<String>>,
    pub checksum: DbtChecksum,
    pub language: Option<String>,
    #[serde(default)]
    pub contract: DbtContract,
    pub created_at: Option<f64>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DbtSeed {
    #[serde(flatten)]
    pub common_attr: CommonAttributes,

    #[serde(flatten)]
    pub base_attr: NodeBaseAttributes,

    // Test Specific Attributes
    pub config: DbtConfig,
    pub root_path: Option<PathBuf>,

    #[serde(flatten)]
    pub other: BTreeMap<String, Value>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DbtUnitTest {
    #[serde(flatten)]
    pub common_attr: CommonAttributes,

    #[serde(flatten)]
    pub base_attr: NodeBaseAttributes,
    /// Unit Test Specific Attributes
    pub config: DbtConfig,
    pub model: String,
    pub given: Vec<Given>,
    pub expect: Expect,
    pub versions: Option<IncludeExclude>,
    pub version: Option<StringOrInteger>,
    pub overrides: Option<Value>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtTest {
    #[serde(flatten)]
    pub common_attr: CommonAttributes,
    #[serde(flatten)]
    pub base_attr: NodeBaseAttributes,

    /// Test Specific Attributes
    pub config: DbtConfig,
    pub column_name: Option<String>,
    pub attached_node: Option<String>,
    pub test_metadata: Option<TestMetadata>,
    pub file_key_name: Option<String>,

    #[serde(flatten)]
    pub other: BTreeMap<String, Value>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct TestMetadata {
    pub name: String,
    pub kwargs: BTreeMap<String, Value>,
    pub namespace: Option<String>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtSnapshot {
    #[serde(flatten)]
    pub common_attr: CommonAttributes,
    #[serde(flatten)]
    pub base_attr: NodeBaseAttributes,

    /// Snapshot Specific Attributes
    pub config: DbtConfig,

    #[serde(flatten)]
    pub other: BTreeMap<String, Value>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtSource {
    #[serde(flatten)]
    pub common_attr: CommonAttributes,

    // Source Specific Attributes
    pub relation_name: Option<String>,
    pub identifier: String,
    pub source_name: String,
    pub columns: BTreeMap<String, DbtColumn>,
    pub config: DbtConfig,
    pub quoting: Option<DbtQuoting>,
    pub source_description: String,
    pub unrendered_config: BTreeMap<String, Value>,
    pub unrendered_database: Option<String>,
    pub unrendered_schema: Option<String>,
    #[serde(default)]
    pub loader: String,
    pub loaded_at_field: Option<String>,
    pub loaded_at_query: Option<String>,
    pub freshness: Option<FreshnessDefinition>,

    #[serde(flatten)]
    pub other: BTreeMap<String, Value>,
}

impl DbtSource {
    pub fn get_base_attr(&self) -> NodeBaseAttributes {
        NodeBaseAttributes {
            relation_name: self.relation_name.clone(),
            // Source do not have alias, but we use alias to represent the source later on.
            alias: self.identifier.clone(),
            ..Default::default()
        }
    }

    pub fn get_loaded_at_field(&self) -> &str {
        self.loaded_at_field
            .as_ref()
            .map(AsRef::as_ref)
            .unwrap_or("")
    }

    pub fn get_loaded_at_query(&self) -> &str {
        self.loaded_at_query
            .as_ref()
            .map(AsRef::as_ref)
            .unwrap_or("")
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtModel {
    #[serde(flatten)]
    pub common_attr: CommonAttributes,

    #[serde(flatten)]
    pub base_attr: NodeBaseAttributes,

    // Model Specific Attributes
    pub config: ManifestModelConfig,
    #[serde(skip_serializing, default)]
    pub introspection: Option<IntrospectionKind>,
    pub version: Option<StringOrInteger>,
    pub latest_version: Option<StringOrInteger>,
    pub constraints: Vec<Constraint>,
    pub deprecation_date: Option<String>,
    pub primary_key: Vec<String>,
    pub time_spine: Option<Value>,

    #[serde(flatten)]
    pub other: BTreeMap<String, Value>,

    // Do not serialize
    #[serde(skip_serializing, default = "default_false")]
    pub is_extended_model: bool,
}
fn default_false() -> bool {
    false
}

/// refer to https://docs.getdbt.com/reference/resource-configs/{field} for documentation
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct ManifestModelConfig {
    pub enabled: Option<bool>,
    pub alias: Option<String>,
    pub schema: Option<String>,
    pub database: Option<String>,
    pub tags: Option<Vec<String>>,
    // need default to ensure None if field is not set
    #[serde(default, deserialize_with = "default_type")]
    pub meta: Option<BTreeMap<String, Value>>,
    pub group: Option<String>,
    pub materialized: Option<DbtMaterialization>,
    pub incremental_strategy: Option<DbtIncrementalStrategy>,
    pub batch_size: Option<DbtBatchSize>,
    pub lookback: Option<i32>,
    pub begin: Option<String>,
    pub persist_docs: Option<PersistDocsConfig>,
    pub post_hook: Option<Hooks>,
    pub pre_hook: Option<Hooks>,
    pub quoting: Option<DbtQuoting>,
    #[serde(default)]
    pub column_types: BTreeMap<String, String>,
    pub full_refresh: Option<bool>,
    pub unique_key: Option<DbtUniqueKey>,
    pub on_schema_change: Option<OnSchemaChange>,
    pub on_configuration_change: Option<OnConfigurationChange>,
    pub grants: Option<BTreeMap<String, Value>>,
    pub packages: Option<Vec<String>>,
    pub docs: Option<DocsConfig>,
    pub contract: Option<DbtContract>,
    pub event_time: Option<String>,
    pub concurrent_batches: Option<bool>,
    pub merge_update_columns: Option<Vec<String>>,
    pub merge_exclude_columns: Option<Vec<String>>,
    #[serde(default)]
    pub access: Access,
    pub table_format: Option<String>,
    #[serde(flatten)]
    pub snowflake_model_config: SnowflakeModelConfig,
    #[serde(flatten)]
    pub bigquery_model_config: BigQueryModelConfig,
    #[serde(flatten)]
    pub databricks_model_config: DatabricksModelConfig,
    // Unsafe Designation
    #[serde(rename = "unsafe")]
    pub unsafe_: Option<bool>,
    pub skip_compile: Option<bool>,
    pub static_analysis: Option<StaticAnalysisKind>,
    pub freshness: Option<ModelFreshness>,
    pub sql_header: Option<String>,
}

impl From<DbtConfig> for ManifestModelConfig {
    fn from(config: DbtConfig) -> Self {
        ManifestModelConfig {
            enabled: config.enabled,
            alias: config.alias,
            schema: config.schema,
            database: config.database,
            tags: config.tags,
            meta: config.meta,
            group: config.group,
            materialized: config.materialized,
            incremental_strategy: config.incremental_strategy,
            batch_size: config.batch_size,
            lookback: config.lookback,
            begin: config.begin,
            persist_docs: config.persist_docs,
            post_hook: config.post_hook,
            pre_hook: config.pre_hook,
            quoting: config.quoting,
            column_types: config.column_types.unwrap_or_default(),
            full_refresh: config.full_refresh,
            unique_key: config.unique_key,
            on_schema_change: config.on_schema_change,
            on_configuration_change: config.on_configuration_change,
            grants: config.grants,
            packages: config.packages,
            docs: config.docs,
            contract: config.contract,
            event_time: config.event_time,
            concurrent_batches: config.concurrent_batches,
            access: config.access.unwrap_or_default(),
            table_format: config.table_format,
            merge_update_columns: config.merge_update_columns,
            merge_exclude_columns: config.merge_exclude_columns,
            snowflake_model_config: SnowflakeModelConfig {
                external_volume: config.external_volume,
                base_location_root: config.base_location_root,
                base_location_subpath: config.base_location_subpath,
                target_lag: config.target_lag,
                snowflake_warehouse: config.snowflake_warehouse,
                refresh_mode: config.refresh_mode,
                initialize: config.initialize,
                tmp_relation_type: config.tmp_relation_type,
                query_tag: config.query_tag,
                automatic_clustering: config.automatic_clustering,
                copy_grants: config.copy_grants,
                secure: config.secure,
            },
            bigquery_model_config: BigQueryModelConfig {
                cluster_by: config.cluster_by,
                partition_by: config.partition_by,
                hours_to_expiration: config.hours_to_expiration,
                labels: config.labels,
                labels_from_meta: config.labels_from_meta,
                kms_key_name: config.kms_key_name,
                require_partition_filter: config.require_partition_filter.unwrap_or(false),
                partition_expiration_days: config.partition_expiration_days,
                grant_access_to: config.grant_access_to,
                partitions: config.partitions,
                enable_refresh: config.enable_refresh,
                refresh_interval_minutes: config.refresh_interval_minutes,
                description: config.description,
                max_staleness: config.max_staleness,
            },
            databricks_model_config: DatabricksModelConfig {
                file_format: config.file_format,
                location_root: config.location_root,
                tblproperties: config.tblproperties,
                include_full_name_in_path: config.include_full_name_in_path,
                auto_liquid_cluster: config.auto_liquid_cluster,
                liquid_clustered_by: config.liquid_clustered_by,
                buckets: config.buckets,
                clustered_by: config.clustered_by.clone(),
                compression: config.compression.clone(),
                catalog: config.catalog.clone(),
                databricks_tags: config.databricks_tags.clone(),
                databricks_compute: config.databricks_compute.clone(),
            },
            unsafe_: config.unsafe_,
            skip_compile: config.skip_compile,
            static_analysis: config.static_analysis,
            freshness: config.model_freshness,
            sql_header: config.sql_header,
        }
    }
}

impl From<ManifestModelConfig> for DbtConfig {
    fn from(config: ManifestModelConfig) -> Self {
        DbtConfig {
            enabled: config.enabled,
            alias: config.alias,
            schema: config.schema,
            database: config.database,
            tags: config.tags,
            meta: config.meta,
            group: config.group,
            materialized: config.materialized,
            incremental_strategy: config.incremental_strategy,
            batch_size: config.batch_size,
            lookback: config.lookback,
            begin: config.begin,
            persist_docs: config.persist_docs,
            post_hook: config.post_hook,
            pre_hook: config.pre_hook,
            quoting: config.quoting,
            column_types: Some(config.column_types),
            full_refresh: config.full_refresh,
            unique_key: config.unique_key,
            on_schema_change: config.on_schema_change,
            on_configuration_change: config.on_configuration_change,
            grants: config.grants,
            packages: config.packages,
            docs: config.docs,
            contract: config.contract,
            event_time: config.event_time,
            concurrent_batches: config.concurrent_batches,
            access: Some(config.access),
            table_format: config.table_format,
            merge_update_columns: config.merge_update_columns,
            merge_exclude_columns: config.merge_exclude_columns,
            partition_by: config.bigquery_model_config.partition_by,
            hours_to_expiration: config.bigquery_model_config.hours_to_expiration,
            labels: config.bigquery_model_config.labels,
            labels_from_meta: config.bigquery_model_config.labels_from_meta,
            kms_key_name: config.bigquery_model_config.kms_key_name,
            require_partition_filter: Some(config.bigquery_model_config.require_partition_filter),
            partition_expiration_days: config.bigquery_model_config.partition_expiration_days,
            grant_access_to: config.bigquery_model_config.grant_access_to,
            file_format: config.databricks_model_config.file_format,
            location_root: config.databricks_model_config.location_root,
            tblproperties: config.databricks_model_config.tblproperties,
            include_full_name_in_path: config.databricks_model_config.include_full_name_in_path,
            unsafe_: config.unsafe_,
            skip_compile: config.skip_compile,
            static_analysis: config.static_analysis,
            model_freshness: config.freshness,
            sql_header: config.sql_header,
            cluster_by: config.bigquery_model_config.cluster_by,
            external_volume: config.snowflake_model_config.external_volume,
            base_location_root: config.snowflake_model_config.base_location_root,
            base_location_subpath: config.snowflake_model_config.base_location_subpath,
            target_lag: config.snowflake_model_config.target_lag,
            snowflake_warehouse: config.snowflake_model_config.snowflake_warehouse,
            refresh_mode: config.snowflake_model_config.refresh_mode,
            initialize: config.snowflake_model_config.initialize,
            tmp_relation_type: config.snowflake_model_config.tmp_relation_type,
            query_tag: config.snowflake_model_config.query_tag,
            automatic_clustering: config.snowflake_model_config.automatic_clustering,
            copy_grants: config.snowflake_model_config.copy_grants,
            secure: config.snowflake_model_config.secure,
            ..Default::default()
        }
    }
}

impl From<DbtManifest> for Nodes {
    fn from(manifest: DbtManifest) -> Self {
        let mut nodes = Nodes::default();
        // Do not put disabled nodes into the nodes, because all things in Nodes object should be enabled.
        for (unique_id, node) in manifest.nodes {
            match node {
                DbtNode::Model(model) => {
                    nodes.models.insert(unique_id, Arc::new(model));
                }
                DbtNode::Test(test) => {
                    nodes.tests.insert(unique_id, Arc::new(test));
                }
                DbtNode::Snapshot(snapshot) => {
                    nodes.snapshots.insert(unique_id, Arc::new(snapshot));
                }
                DbtNode::Seed(seed) => {
                    nodes.seeds.insert(unique_id, Arc::new(seed));
                }
                DbtNode::Operation(_) => {}
            }
        }
        for (unique_id, source) in manifest.sources {
            nodes.sources.insert(unique_id, Arc::new(source));
        }
        for (unique_id, unit_test) in manifest.unit_tests {
            nodes.unit_tests.insert(unique_id, Arc::new(unit_test));
        }

        nodes
    }
}

#[cfg(test)]
mod tests {
    use super::ManifestModelConfig;
    use serde::Deserialize;

    #[test]
    fn test_deserialize_wo_meta() {
        let config = serde_json::json!({
            "enabled": true,
            // "meta" is missing
        });

        let config = ManifestModelConfig::deserialize(config);
        if let Err(err) = config {
            panic!(
                "Could not deserialize and failed with the following error: {}",
                err
            );
        }
    }
}
