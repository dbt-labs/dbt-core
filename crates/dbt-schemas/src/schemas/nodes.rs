use core::fmt;
use std::{any::Any, collections::BTreeMap, fmt::Display, path::PathBuf, sync::Arc};

use dbt_common::{err, io_args::StaticAnalysisKind, ErrorCode, FsResult};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use serde_with::skip_serializing_none;

use crate::schemas::{
    common::{
        Access, DbtChecksum, DbtContract, DbtIncrementalStrategy, DbtMaterialization, Expect,
        FreshnessDefinition, Given, IncludeExclude, NodeDependsOn, ResolvedQuoting,
    },
    dbt_column::DbtColumn,
    macros::DbtMacro,
    manifest::{DbtExposure, DbtMetric, DbtSavedQuery, DbtSemanticModel},
    project::{
        DataTestConfig, ModelConfig, SeedConfig, SnapshotConfig, SnapshotMetaColumnNames,
        SourceConfig, UnitTestConfig,
    },
    properties::{ModelConstraint, ModelFreshness},
    ref_and_source::{DbtRef, DbtSourceWrapper},
    serde::StringOrInteger,
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

/// A wrapper enum that represents different types of dbt nodes.
///
/// This enum uses serde's tag-based deserialization to automatically determine
/// the correct variant based on the "resource_type" field in the JSON.
/// The resource_type values are converted to snake_case for matching.
///
/// # Example
///
/// ```rust
///
/// // Deserialize a node from Jinja
/// let node = InternalDbtNodeWrapper::deserialize(value).unwrap();
///
/// // Access the underlying node attributes
/// let attributes = node.as_internal_node();
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "resource_type")]
#[serde(rename_all = "snake_case")]
pub enum InternalDbtNodeWrapper {
    Model(DbtModel),
    Seed(DbtSeed),
    Test(DbtTest),
    UnitTest(DbtUnitTest),
    Source(DbtSource),
    Snapshot(DbtSnapshot),
}

impl InternalDbtNodeWrapper {
    /// Returns a reference to the underlying node as a trait object.
    ///
    /// This method allows accessing common functionality across all node types
    /// through the `InternalDbtNodeAttributes` trait.
    ///
    /// # Returns
    ///
    /// A reference to the node implementing `InternalDbtNodeAttributes`
    ///
    /// # Examples
    ///
    /// ```rust
    /// let node = InternalDbtNodeWrapper::Model(some_model);
    /// let attributes = node.as_internal_node();
    /// println!("Node name: {}", attributes.name());
    /// ```
    pub fn as_internal_node(&self) -> &dyn InternalDbtNodeAttributes {
        match self {
            InternalDbtNodeWrapper::Model(model) => model,
            InternalDbtNodeWrapper::Seed(seed) => seed,
            InternalDbtNodeWrapper::Test(test) => test,
            InternalDbtNodeWrapper::UnitTest(unit_test) => unit_test,
            InternalDbtNodeWrapper::Source(source) => source,
            InternalDbtNodeWrapper::Snapshot(snapshot) => snapshot,
        }
    }
}

pub trait InternalDbtNode: Any + Send + Sync + fmt::Debug {
    fn common(&self) -> &CommonAttributes;
    fn base(&self) -> NodeBaseAttributes;
    fn base_mut(&mut self) -> Option<&mut NodeBaseAttributes>;
    fn common_mut(&mut self) -> &mut CommonAttributes;
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

    fn is_test(&self) -> bool {
        self.resource_type() == "test"
    }

    // Incremental strategy validation
    fn warn_on_microbatch(&self) -> FsResult<()> {
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
            "node_info":{
                "node_name": self.common().name,
                "unique_id": self.common().unique_id,
                "node_finished_at": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f").to_string(),
                "node_status": status
            },
        })
    }
}

pub trait InternalDbtNodeAttributes: InternalDbtNode {
    // Required Fields
    fn materialized(&self) -> DbtMaterialization;
    fn quoting(&self) -> ResolvedQuoting;
    fn tags(&self) -> Vec<String>;
    fn meta(&self) -> BTreeMap<String, Value>;
    fn static_analysis(&self) -> StaticAnalysisKind {
        StaticAnalysisKind::On
    }
    // Setters
    fn set_quoting(&mut self, quoting: ResolvedQuoting);
    fn set_static_analysis(&mut self, static_analysis: StaticAnalysisKind);

    // Optional Fields
    fn get_access(&self) -> Option<Access> {
        None
    }
    fn get_group(&self) -> Option<String> {
        None
    }

    // TO BE DEPRECATED
    fn serialized_config(&self) -> Value;
}

impl InternalDbtNode for DbtModel {
    fn common(&self) -> &CommonAttributes {
        &self.common_attr
    }

    fn base(&self) -> NodeBaseAttributes {
        self.base_attr.clone()
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
            self.deprecated_config == other_model.deprecated_config
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

    fn warn_on_microbatch(&self) -> FsResult<()> {
        if let Some(DbtIncrementalStrategy::Microbatch) = self.incremental_strategy {
            return err!(
                code => ErrorCode::UnsupportedFeature,
                loc => self.common().path.clone(),
                "Microbatch incremental strategy is not supported. Use --exclude config.incremental_strategy:microbatch to exclude these models."
            );
        }
        Ok(())
    }
}

impl InternalDbtNodeAttributes for DbtModel {
    fn materialized(&self) -> DbtMaterialization {
        self.materialized.clone()
    }
    fn quoting(&self) -> ResolvedQuoting {
        self.quoting
    }
    fn static_analysis(&self) -> StaticAnalysisKind {
        self.static_analysis
    }
    fn set_quoting(&mut self, quoting: ResolvedQuoting) {
        self.quoting = quoting;
    }
    fn set_static_analysis(&mut self, static_analysis: StaticAnalysisKind) {
        self.static_analysis = static_analysis;
    }
    fn tags(&self) -> Vec<String> {
        self.tags.clone()
    }
    fn meta(&self) -> BTreeMap<String, Value> {
        self.meta.clone()
    }
    fn get_access(&self) -> Option<Access> {
        Some(self.access.clone())
    }
    fn get_group(&self) -> Option<String> {
        self.group.clone()
    }
    fn serialized_config(&self) -> Value {
        serde_json::to_value(&self.deprecated_config).expect("Failed to serialize DbtModel")
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

    fn serialize_inner(&self) -> Value {
        serde_json::to_value(self).expect("Failed to serialize DbtSeed")
    }

    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_model) = other.as_any().downcast_ref::<DbtSeed>() {
            self.deprecated_config == other_model.deprecated_config
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

impl InternalDbtNodeAttributes for DbtSeed {
    fn materialized(&self) -> DbtMaterialization {
        self.materialized.clone()
    }
    fn quoting(&self) -> ResolvedQuoting {
        self.quoting
    }
    fn set_quoting(&mut self, quoting: ResolvedQuoting) {
        self.quoting = quoting;
    }
    fn set_static_analysis(&mut self, _static_analysis: StaticAnalysisKind) {
        unimplemented!()
    }
    fn tags(&self) -> Vec<String> {
        self.tags.clone()
    }
    fn meta(&self) -> BTreeMap<String, Value> {
        self.meta.clone()
    }
    fn serialized_config(&self) -> Value {
        serde_json::to_value(&self.deprecated_config).expect("Failed to serialize DbtModel")
    }
}

impl InternalDbtNode for DbtTest {
    fn common(&self) -> &CommonAttributes {
        &self.common_attr
    }

    fn base(&self) -> NodeBaseAttributes {
        self.base_attr.clone()
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
            self.deprecated_config.enabled == other.deprecated_config.enabled
                && self.deprecated_config.alias == other.deprecated_config.alias
                && self.deprecated_config.database == other.deprecated_config.database
                && self.deprecated_config.tags == other.deprecated_config.tags
                && self.deprecated_config.meta == other.deprecated_config.meta
                && self.deprecated_config.group == other.deprecated_config.group
                && self.deprecated_config.quoting == other.deprecated_config.quoting
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

impl InternalDbtNodeAttributes for DbtTest {
    fn materialized(&self) -> DbtMaterialization {
        DbtMaterialization::Test
    }
    fn quoting(&self) -> ResolvedQuoting {
        self.quoting
    }
    fn set_quoting(&mut self, quoting: ResolvedQuoting) {
        self.quoting = quoting;
    }
    fn set_static_analysis(&mut self, static_analysis: StaticAnalysisKind) {
        self.static_analysis = static_analysis;
    }
    fn tags(&self) -> Vec<String> {
        self.tags.clone()
    }
    fn meta(&self) -> BTreeMap<String, Value> {
        self.meta.clone()
    }
    fn serialized_config(&self) -> Value {
        serde_json::to_value(&self.deprecated_config).expect("Failed to serialize DbtModel")
    }
}

impl InternalDbtNode for DbtUnitTest {
    fn common(&self) -> &CommonAttributes {
        &self.common_attr
    }

    fn base(&self) -> NodeBaseAttributes {
        self.base_attr.clone()
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
            self.deprecated_config == other.deprecated_config
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

impl InternalDbtNodeAttributes for DbtUnitTest {
    fn materialized(&self) -> DbtMaterialization {
        DbtMaterialization::Unit
    }
    fn quoting(&self) -> ResolvedQuoting {
        self.quoting
    }
    fn set_quoting(&mut self, quoting: ResolvedQuoting) {
        self.quoting = quoting;
    }
    fn set_static_analysis(&mut self, static_analysis: StaticAnalysisKind) {
        self.static_analysis = static_analysis;
    }
    fn tags(&self) -> Vec<String> {
        self.tags.clone()
    }
    fn meta(&self) -> BTreeMap<String, Value> {
        self.meta.clone()
    }
    fn serialized_config(&self) -> Value {
        serde_json::to_value(&self.deprecated_config).expect("Failed to serialize DbtModel")
    }
}

impl InternalDbtNode for DbtSource {
    fn common(&self) -> &CommonAttributes {
        &self.common_attr
    }

    fn base(&self) -> NodeBaseAttributes {
        self.get_base_attr()
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
            self.deprecated_config == other_source.deprecated_config
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
                && self.deprecated_config == other_source.deprecated_config
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

impl InternalDbtNodeAttributes for DbtSource {
    fn materialized(&self) -> DbtMaterialization {
        DbtMaterialization::External
    }
    fn static_analysis(&self) -> StaticAnalysisKind {
        self.static_analysis
    }
    fn quoting(&self) -> ResolvedQuoting {
        self.quoting
    }
    fn set_quoting(&mut self, quoting: ResolvedQuoting) {
        self.quoting = quoting;
    }
    fn set_static_analysis(&mut self, static_analysis: StaticAnalysisKind) {
        self.static_analysis = static_analysis;
    }
    fn tags(&self) -> Vec<String> {
        self.tags.clone()
    }
    fn meta(&self) -> BTreeMap<String, Value> {
        self.meta.clone()
    }
    fn serialized_config(&self) -> Value {
        serde_json::to_value(&self.deprecated_config).expect("Failed to serialize DbtModel")
    }
}

impl InternalDbtNode for DbtSnapshot {
    fn common(&self) -> &CommonAttributes {
        &self.common_attr
    }
    fn base(&self) -> NodeBaseAttributes {
        self.base_attr.clone()
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
            self.deprecated_config == other_snapshot.deprecated_config
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

impl InternalDbtNodeAttributes for DbtSnapshot {
    fn materialized(&self) -> DbtMaterialization {
        self.materialized.clone()
    }
    fn static_analysis(&self) -> StaticAnalysisKind {
        self.static_analysis
    }
    fn quoting(&self) -> ResolvedQuoting {
        self.quoting
    }
    fn set_quoting(&mut self, quoting: ResolvedQuoting) {
        self.quoting = quoting;
    }
    fn set_static_analysis(&mut self, static_analysis: StaticAnalysisKind) {
        self.static_analysis = static_analysis;
    }
    fn tags(&self) -> Vec<String> {
        self.tags.clone()
    }
    fn meta(&self) -> BTreeMap<String, Value> {
        self.meta.clone()
    }
    fn serialized_config(&self) -> Value {
        serde_json::to_value(&self.deprecated_config).expect("Failed to serialize DbtModel")
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
    fn resource_type(&self) -> &str {
        "exposure"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn serialize_inner(&self) -> Value {
        serde_json::to_value(self).expect("Failed to serialize DbtExposure")
    }
    fn has_same_config(&self, _other: &dyn InternalDbtNode) -> bool {
        unimplemented!()
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
    fn resource_type(&self) -> &str {
        "metric"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn serialize_inner(&self) -> Value {
        serde_json::to_value(self).expect("Failed to serialize DbtMetric")
    }
    fn has_same_config(&self, _other: &dyn InternalDbtNode) -> bool {
        unimplemented!()
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

    pub fn get_node(&self, unique_id: &str) -> Option<&dyn InternalDbtNodeAttributes> {
        self.models
            .get(unique_id)
            .map(|n| Arc::as_ref(n) as &dyn InternalDbtNodeAttributes)
            .or_else(|| {
                self.seeds
                    .get(unique_id)
                    .map(|n| Arc::as_ref(n) as &dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.tests
                    .get(unique_id)
                    .map(|n| Arc::as_ref(n) as &dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.unit_tests
                    .get(unique_id)
                    .map(|n| Arc::as_ref(n) as &dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.sources
                    .get(unique_id)
                    .map(|n| Arc::as_ref(n) as &dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.snapshots
                    .get(unique_id)
                    .map(|n| Arc::as_ref(n) as &dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.analyses
                    .get(unique_id)
                    .map(|n| Arc::as_ref(n) as &dyn InternalDbtNodeAttributes)
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

    pub fn iter(&self) -> impl Iterator<Item = (&String, &dyn InternalDbtNodeAttributes)> + '_ {
        self.models
            .iter()
            .map(|(id, node)| (id, Arc::as_ref(node) as &dyn InternalDbtNodeAttributes))
            .chain(
                self.seeds
                    .iter()
                    .map(|(id, node)| (id, Arc::as_ref(node) as &dyn InternalDbtNodeAttributes)),
            )
            .chain(
                self.tests
                    .iter()
                    .map(|(id, node)| (id, Arc::as_ref(node) as &dyn InternalDbtNodeAttributes)),
            )
            .chain(
                self.unit_tests
                    .iter()
                    .map(|(id, node)| (id, Arc::as_ref(node) as &dyn InternalDbtNodeAttributes)),
            )
            .chain(
                self.sources
                    .iter()
                    .map(|(id, node)| (id, Arc::as_ref(node) as &dyn InternalDbtNodeAttributes)),
            )
            .chain(
                self.snapshots
                    .iter()
                    .map(|(id, node)| (id, Arc::as_ref(node) as &dyn InternalDbtNodeAttributes)),
            )
            .chain(
                self.analyses
                    .iter()
                    .map(|(id, node)| (id, Arc::as_ref(node) as &dyn InternalDbtNodeAttributes)),
            )
    }

    pub fn into_iter(
        &self,
    ) -> impl Iterator<Item = (String, Arc<dyn InternalDbtNodeAttributes>)> + '_ {
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

    pub fn iter_values_mut(
        &mut self,
    ) -> impl Iterator<Item = &mut dyn InternalDbtNodeAttributes> + '_ {
        let map_models = self
            .models
            .values_mut()
            .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNodeAttributes);
        let map_seeds = self
            .seeds
            .values_mut()
            .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNodeAttributes);
        let map_tests = self
            .tests
            .values_mut()
            .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNodeAttributes);
        let map_unit_tests = self
            .unit_tests
            .values_mut()
            .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNodeAttributes);
        let map_sources = self
            .sources
            .values_mut()
            .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNodeAttributes);
        let map_snapshots = self
            .snapshots
            .values_mut()
            .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNodeAttributes);
        let map_analyses = self
            .analyses
            .values_mut()
            .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNodeAttributes);

        map_models
            .chain(map_seeds)
            .chain(map_tests)
            .chain(map_unit_tests)
            .chain(map_sources)
            .chain(map_snapshots)
            .chain(map_analyses)
    }

    pub fn get_value_mut(&mut self, unique_id: &str) -> Option<&mut dyn InternalDbtNodeAttributes> {
        self.models
            .get_mut(unique_id)
            .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNodeAttributes)
            .or_else(|| {
                self.seeds
                    .get_mut(unique_id)
                    .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.tests
                    .get_mut(unique_id)
                    .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.unit_tests
                    .get_mut(unique_id)
                    .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.sources
                    .get_mut(unique_id)
                    .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.snapshots
                    .get_mut(unique_id)
                    .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.analyses
                    .get_mut(unique_id)
                    .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNodeAttributes)
            })
    }

    pub fn get_by_relation_name(
        &self,
        relation_name: &str,
    ) -> Option<&dyn InternalDbtNodeAttributes> {
        self.models
            .values()
            .find(|model| model.base().relation_name == Some(relation_name.to_string()))
            .map(|arc| Arc::as_ref(arc) as &dyn InternalDbtNodeAttributes)
            .or_else(|| {
                self.seeds
                    .values()
                    .find(|seed| seed.base().relation_name == Some(relation_name.to_string()))
                    .map(|arc| Arc::as_ref(arc) as &dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.tests
                    .values()
                    .find(|test| test.base().relation_name == Some(relation_name.to_string()))
                    .map(|arc| Arc::as_ref(arc) as &dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.unit_tests
                    .values()
                    .find(|unit_test| {
                        unit_test.base().relation_name == Some(relation_name.to_string())
                    })
                    .map(|arc| Arc::as_ref(arc) as &dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.sources
                    .values()
                    .find(|source| source.base().relation_name == Some(relation_name.to_string()))
                    .map(|arc| Arc::as_ref(arc) as &dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.snapshots
                    .values()
                    .find(|snapshot| {
                        snapshot.base().relation_name == Some(relation_name.to_string())
                    })
                    .map(|arc| Arc::as_ref(arc) as &dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.analyses
                    .values()
                    .find(|analysis| {
                        analysis.base().relation_name == Some(relation_name.to_string())
                    })
                    .map(|arc| Arc::as_ref(arc) as &dyn InternalDbtNodeAttributes)
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
            if let DbtMaterialization::Unknown(custom) = node.materialized() {
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

fn upcast<T: InternalDbtNodeAttributes + 'static>(
    arc: Arc<T>,
) -> Arc<dyn InternalDbtNodeAttributes> {
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

    // [Start] Previously config fields
    pub materialized: DbtMaterialization,
    pub quoting: ResolvedQuoting,
    pub tags: Vec<String>,
    pub meta: BTreeMap<String, Value>,
    // [End]

    // To be deprecated
    #[serde(rename = "config")]
    pub deprecated_config: SeedConfig,

    // Test Specific Attributes
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

    // [Start] Previously config fields
    pub quoting: ResolvedQuoting,
    pub static_analysis: StaticAnalysisKind,
    pub tags: Vec<String>,
    pub meta: BTreeMap<String, Value>,
    // [End]

    // To be deprecated
    #[serde(rename = "config")]
    pub deprecated_config: UnitTestConfig,

    /// Unit Test Specific Attributes
    pub model: String,
    pub given: Vec<Given>,
    pub expect: Expect,
    pub versions: Option<IncludeExclude>,
    pub version: Option<StringOrInteger>,
    pub overrides: Option<Value>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DbtTest {
    #[serde(flatten)]
    pub common_attr: CommonAttributes,
    #[serde(flatten)]
    pub base_attr: NodeBaseAttributes,

    // [Start] Previously config fields
    pub quoting: ResolvedQuoting,
    pub static_analysis: StaticAnalysisKind,
    pub tags: Vec<String>,
    pub meta: BTreeMap<String, Value>,
    // [End]

    // To be deprecated
    #[serde(rename = "config")]
    pub deprecated_config: DataTestConfig,

    /// Test Specific Attributes
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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DbtSnapshot {
    #[serde(flatten)]
    pub common_attr: CommonAttributes,
    #[serde(flatten)]
    pub base_attr: NodeBaseAttributes,

    // [Start] Previously config fields
    pub materialized: DbtMaterialization,
    pub quoting: ResolvedQuoting,
    pub static_analysis: StaticAnalysisKind,
    pub tags: Vec<String>,
    pub meta: BTreeMap<String, Value>,
    pub snapshot_meta_column_names: SnapshotMetaColumnNames,
    // [End]
    /// To be deprecated
    #[serde(rename = "config")]
    pub deprecated_config: SnapshotConfig,

    #[serde(flatten)]
    pub other: BTreeMap<String, Value>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DbtSource {
    #[serde(flatten)]
    pub common_attr: CommonAttributes,

    // [Start] Previously config fields
    pub quoting: ResolvedQuoting,
    pub static_analysis: StaticAnalysisKind,
    pub tags: Vec<String>,
    pub meta: BTreeMap<String, Value>,
    // [End]

    // Source Specific Attributes
    pub relation_name: Option<String>,
    pub identifier: String,
    pub source_name: String,
    pub columns: BTreeMap<String, DbtColumn>,

    // To be deprecated
    #[serde(rename = "config")]
    pub deprecated_config: SourceConfig,

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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DbtModel {
    #[serde(flatten)]
    pub common_attr: CommonAttributes,

    #[serde(flatten)]
    pub base_attr: NodeBaseAttributes,

    // [Start] Previously config fields
    pub materialized: DbtMaterialization,
    pub quoting: ResolvedQuoting,
    pub access: Access,
    pub group: Option<String>,
    pub tags: Vec<String>,
    pub meta: BTreeMap<String, Value>,
    pub enabled: bool,
    pub static_analysis: StaticAnalysisKind,
    pub contract: Option<DbtContract>,
    pub incremental_strategy: Option<DbtIncrementalStrategy>,
    pub freshness: Option<ModelFreshness>,
    // [End]

    // TO BE DEPRECATED
    #[serde(rename = "config")]
    pub deprecated_config: ModelConfig,

    #[serde(skip_serializing, default)]
    pub introspection: Option<IntrospectionKind>,
    pub version: Option<StringOrInteger>,
    pub latest_version: Option<StringOrInteger>,
    pub constraints: Vec<ModelConstraint>,
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

#[cfg(test)]
mod tests {
    use super::ModelConfig;
    use serde::Deserialize;

    #[test]
    fn test_deserialize_wo_meta() {
        let config = serde_json::json!({
            "enabled": true,
            // "meta" is missing
        });

        let config = ModelConfig::deserialize(config);
        if let Err(err) = config {
            panic!(
                "Could not deserialize and failed with the following error: {}",
                err
            );
        }
    }
}
