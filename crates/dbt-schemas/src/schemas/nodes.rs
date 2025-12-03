use core::fmt;
use std::str::FromStr;
use std::{any::Any, collections::BTreeMap, fmt::Display, path::PathBuf, sync::Arc};

use chrono::{DateTime, Utc};
use dbt_common::adapter::AdapterType;
use dbt_common::io_args::StaticAnalysisOffReason;
use dbt_common::tracing::emit::{emit_error_log_message, emit_warn_log_message};
use dbt_common::{ErrorCode, FsResult, err, io_args::StaticAnalysisKind};
use dbt_telemetry::{ExecutionPhase, NodeEvaluated, NodeProcessed, NodeType};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
type YmlValue = dbt_serde_yaml::Value;
use crate::schemas::common::{NodeInfo, NodeInfoWrapper, PersistDocsConfig, hooks_equal};
use crate::schemas::dbt_column::{DbtColumnRef, deserialize_dbt_columns, serialize_dbt_columns};
use crate::schemas::manifest::{BigqueryClusterConfig, GrantAccessToTarget, PartitionConfig};
use crate::schemas::project::{WarehouseSpecificNodeConfig, same_warehouse_config};
use crate::schemas::serde::StringOrArrayOfStrings;
use crate::schemas::{
    common::{
        Access, DbtChecksum, DbtContract, DbtIncrementalStrategy, DbtMaterialization, Expect,
        FreshnessDefinition, Given, IncludeExclude, NodeDependsOn, ResolvedQuoting, ScheduleConfig,
    },
    macros::DbtMacro,
    manifest::common::DbtOwner,
    manifest::semantic_model::NodeRelation,
    manifest::{DbtMetric, DbtSavedQuery, DbtSemanticModel},
    project::{
        DataTestConfig, ExposureConfig, FunctionConfig, ModelConfig, SeedConfig, SnapshotConfig,
        SnapshotMetaColumnNames, SourceConfig, UnitTestConfig,
    },
    properties::{
        FunctionArgument, FunctionReturnType, ModelConstraint, ModelFreshness, UnitTestOverrides,
    },
    ref_and_source::{DbtRef, DbtSourceWrapper},
    serde::StringOrInteger,
};
use dbt_serde_yaml::{Spanned, UntaggedEnumDeserialize};

#[derive(
    Default, Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum IntrospectionKind {
    #[default]
    None,
    Execute,
    This,
    UpstreamSchema,
    InternalSchema,
    ExternalSchema,
    Unknown,
}

impl IntrospectionKind {
    pub fn is_unsafe(&self) -> bool {
        matches!(
            self,
            IntrospectionKind::Execute
                | IntrospectionKind::InternalSchema
                | IntrospectionKind::ExternalSchema
                | IntrospectionKind::This
                | IntrospectionKind::Unknown
        )
    }
}

impl Display for IntrospectionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IntrospectionKind::None => write!(f, "none"),
            IntrospectionKind::Execute => write!(f, "execute"),
            IntrospectionKind::UpstreamSchema => write!(f, "upstream_schema"),
            IntrospectionKind::InternalSchema => write!(f, "internal_schema"),
            IntrospectionKind::ExternalSchema => write!(f, "external_schema"),
            IntrospectionKind::This => write!(f, "this"),
            IntrospectionKind::Unknown => write!(f, "unknown"),
        }
    }
}

impl FromStr for IntrospectionKind {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "none" => Ok(IntrospectionKind::None),
            "execute" => Ok(IntrospectionKind::Execute),
            "upstream_schema" => Ok(IntrospectionKind::UpstreamSchema),
            "internal_schema" => Ok(IntrospectionKind::InternalSchema),
            "external_schema" => Ok(IntrospectionKind::ExternalSchema),
            "this" => Ok(IntrospectionKind::This),
            _ => Err(()),
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
/// let node = minijinja_value_to_typed_struct::<InternalDbtNodeWrapper>(value).unwrap();
///
/// // Access the underlying node attributes
/// let attributes = node.as_internal_node();
/// ```
#[derive(Debug, Clone, Serialize, UntaggedEnumDeserialize)]
#[serde(tag = "resource_type")]
#[serde(rename_all = "snake_case")]
pub enum InternalDbtNodeWrapper {
    Model(Box<DbtModel>),
    Seed(Box<DbtSeed>),
    Test(Box<DbtTest>),
    UnitTest(Box<DbtUnitTest>),
    Source(Box<DbtSource>),
    Snapshot(Box<DbtSnapshot>),
    Exposure(Box<DbtExposure>),
    Function(Box<DbtFunction>),
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
            InternalDbtNodeWrapper::Model(model) => model.as_ref(),
            InternalDbtNodeWrapper::Seed(seed) => seed.as_ref(),
            InternalDbtNodeWrapper::Test(test) => test.as_ref(),
            InternalDbtNodeWrapper::UnitTest(unit_test) => unit_test.as_ref(),
            InternalDbtNodeWrapper::Source(source) => source.as_ref(),
            InternalDbtNodeWrapper::Snapshot(snapshot) => snapshot.as_ref(),
            InternalDbtNodeWrapper::Exposure(exposure) => exposure.as_ref(),
            InternalDbtNodeWrapper::Function(function) => function.as_ref(),
        }
    }
}

pub trait InternalDbtNode: Any + Send + Sync + fmt::Debug {
    fn common(&self) -> &CommonAttributes;
    fn base(&self) -> &NodeBaseAttributes;
    fn base_mut(&mut self) -> &mut NodeBaseAttributes;
    fn common_mut(&mut self) -> &mut CommonAttributes;
    fn version(&self) -> Option<StringOrInteger> {
        None
    }
    fn latest_version(&self) -> Option<StringOrInteger> {
        None
    }
    fn event_time(&self) -> Option<String> {
        None
    }
    fn is_extended_model(&self) -> bool {
        false
    }
    fn is_versioned(&self) -> bool {
        false
    }
    fn defined_at(&self) -> Option<&dbt_common::CodeLocation> {
        None
    }
    fn resource_type(&self) -> NodeType;
    fn as_any(&self) -> &dyn Any;
    fn serialize(&self) -> YmlValue {
        self.serialize_with_mode(crate::schemas::serialization_utils::SerializationMode::OmitNone)
    }
    fn serialize_for_jinja(&self) -> YmlValue {
        self.serialize_with_mode(crate::schemas::serialization_utils::SerializationMode::KeepNone)
    }
    fn serialize_with_mode(
        &self,
        mode: crate::schemas::serialization_utils::SerializationMode,
    ) -> YmlValue {
        let mut ret = self.serialize_inner(mode);
        if let YmlValue::Mapping(ref mut map, _) = ret {
            map.insert(
                YmlValue::string("resource_type".to_string()),
                YmlValue::string(self.resource_type().as_static_ref().to_string()),
            );
        }
        ret
    }
    fn serialize_inner(
        &self,
        mode: crate::schemas::serialization_utils::SerializationMode,
    ) -> YmlValue;

    // Selector functions
    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool;
    fn has_same_content(&self, other: &dyn InternalDbtNode) -> bool;
    fn set_detected_introspection(&mut self, introspection: IntrospectionKind);
    fn introspection(&self) -> IntrospectionKind {
        IntrospectionKind::None
    }

    fn is_test(&self) -> bool {
        self.resource_type() == NodeType::Test
    }

    // Some node types are never considered new even if the previous state is missing. See
    // the same_contents method for Exposure, SavedQuery, SemanticModel and Metrics in
    // mantle: https://github.com/dbt-labs/dbt-mantle/blob/1a251ee081adf4c7af2ba38e7797b04e69d2a15f/core/dbt/contracts/graph/nodes.py
    fn is_never_new_if_previous_missing(&self) -> bool {
        self.resource_type() == NodeType::Exposure
            || self.resource_type() == NodeType::SavedQuery
            || self.resource_type() == NodeType::SemanticModel
            || self.resource_type() == NodeType::Metric
            || self.is_test() // As per Chenyu tests are a part of this list because of a mantle vs fusion difference in test naming which causes false positives. generation.
    }

    // Incremental strategy validation
    fn warn_on_microbatch(&self) -> FsResult<()> {
        Ok(())
    }

    fn get_node_start_data(&self, node_started_at: DateTime<Utc>) -> NodeInfoWrapper {
        NodeInfoWrapper {
            unique_id: None,
            skipped_nodes: None,
            defined_at: self.defined_at().cloned(),
            node_info: NodeInfo {
                node_name: self.common().name.clone(),
                unique_id: self.common().unique_id.clone(),
                node_started_at: Some(node_started_at.format("%Y-%m-%dT%H:%M:%S%.6f").to_string()),
                node_finished_at: None,
                node_status: "executing".to_string(),
            },
        }
    }

    fn get_node_end_data(
        &self,
        status: &str,
        node_started_at: DateTime<Utc>,
        node_finished_at: DateTime<Utc>,
    ) -> NodeInfoWrapper {
        NodeInfoWrapper {
            unique_id: None,
            skipped_nodes: None,
            defined_at: self.defined_at().cloned(),
            node_info: NodeInfo {
                node_name: self.common().name.clone(),
                unique_id: self.common().unique_id.clone(),
                node_started_at: Some(node_started_at.format("%Y-%m-%dT%H:%M:%S%.6f").to_string()),
                node_finished_at: Some(
                    node_finished_at.format("%Y-%m-%dT%H:%M:%S%.6f").to_string(),
                ),
                node_status: status.to_string(),
            },
        }
    }

    fn get_node_evaluated_event(&self, phase: ExecutionPhase, in_dir: &PathBuf) -> NodeEvaluated {
        let common = self.common();
        let base = self.base();
        let node_type = self.resource_type();

        let (database, schema, identifier) = (
            base.database.clone(),
            base.schema.clone(),
            Some(base.alias.clone()),
        );

        let custom_materialization = if let DbtMaterialization::Unknown(custom) = &base.materialized
        {
            Some(custom.clone())
        } else {
            None
        };

        let (relative_path, defined_at_line, defined_at_column) = self.defined_at().map_or_else(
            || (common.original_file_path.display().to_string(), None, None),
            |defined_at| {
                let relative_path = if defined_at.file.is_absolute() {
                    defined_at
                        .file
                        .strip_prefix(in_dir)
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|_| defined_at.file.display().to_string())
                } else {
                    defined_at.file.display().to_string()
                };

                (
                    relative_path,
                    Some(defined_at.line as u32),
                    Some(defined_at.col as u32),
                )
            },
        );

        let node_checksum = match &common.checksum {
            DbtChecksum::String(s) => s.clone(),
            DbtChecksum::Object(o) => o.checksum.clone(),
        };

        NodeEvaluated::start(
            common.unique_id.clone(),
            common.name.clone(),
            Some(database),
            Some(schema),
            identifier,
            Some((&base.materialized).into()),
            custom_materialization,
            node_type,
            phase,
            relative_path,
            defined_at_line,
            defined_at_column,
            node_checksum,
        )
    }

    fn get_node_processed_event(
        &self,
        last_phase: Option<ExecutionPhase>,
        in_dir: &PathBuf,
        in_selection: bool,
    ) -> NodeProcessed {
        let common = self.common();
        let base = self.base();
        let node_type = self.resource_type();

        let (database, schema, identifier) = (
            base.database.clone(),
            base.schema.clone(),
            Some(base.alias.clone()),
        );

        let custom_materialization = if let DbtMaterialization::Unknown(custom) = &base.materialized
        {
            Some(custom.clone())
        } else {
            None
        };

        let (relative_path, defined_at_line, defined_at_column) = self.defined_at().map_or_else(
            || (common.original_file_path.display().to_string(), None, None),
            |defined_at| {
                let relative_path = if defined_at.file.is_absolute() {
                    defined_at
                        .file
                        .strip_prefix(in_dir)
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|_| defined_at.file.display().to_string())
                } else {
                    defined_at.file.display().to_string()
                };

                (
                    relative_path,
                    Some(defined_at.line as u32),
                    Some(defined_at.col as u32),
                )
            },
        );

        let node_checksum = match &common.checksum {
            DbtChecksum::String(s) => s.clone(),
            DbtChecksum::Object(o) => o.checksum.clone(),
        };

        NodeProcessed::start(
            common.unique_id.clone(),
            common.name.clone(),
            Some(database),
            Some(schema),
            identifier,
            Some((&base.materialized).into()),
            custom_materialization,
            node_type,
            last_phase,
            relative_path,
            defined_at_line,
            defined_at_column,
            node_checksum,
            in_selection,
        )
    }
}

pub trait InternalDbtNodeAttributes: InternalDbtNode {
    // Required Fields
    fn skip_generate_database_name_macro(&self) -> bool {
        false
    }

    fn database(&self) -> String {
        self.base().database.clone()
    }

    fn skip_generate_schema_name_macro(&self) -> bool {
        false
    }

    fn schema(&self) -> String {
        self.base().schema.clone()
    }

    fn unique_id(&self) -> String {
        self.common().unique_id.clone()
    }

    fn name(&self) -> String {
        self.common().name.clone()
    }

    fn alias(&self) -> String {
        self.base().alias.clone()
    }

    fn path(&self) -> PathBuf {
        self.common().path.clone()
    }

    fn original_file_path(&self) -> PathBuf {
        self.common().original_file_path.clone()
    }

    fn package_name(&self) -> String {
        self.common().package_name.clone()
    }

    fn materialized(&self) -> DbtMaterialization {
        self.base().materialized.clone()
    }

    fn quoting(&self) -> ResolvedQuoting {
        self.base().quoting
    }

    fn tags(&self) -> Vec<String> {
        self.common().tags.clone()
    }

    fn meta(&self) -> BTreeMap<String, YmlValue> {
        self.common().meta.clone()
    }

    fn static_analysis(&self) -> Spanned<StaticAnalysisKind> {
        self.base().static_analysis.clone()
    }

    fn static_analysis_enabled(&self) -> Spanned<bool> {
        self.static_analysis().map(|static_analysis| {
            static_analysis == StaticAnalysisKind::On
                || static_analysis == StaticAnalysisKind::Unsafe
        })
    }

    fn static_analysis_off_reason(&self) -> Option<StaticAnalysisOffReason> {
        self.base().static_analysis_off_reason
    }
    // Setters

    fn set_quoting(&mut self, quoting: ResolvedQuoting) {
        self.base_mut().quoting = quoting;
    }

    fn set_static_analysis(&mut self, static_analysis: Spanned<StaticAnalysisKind>) {
        self.base_mut().static_analysis = static_analysis;
    }

    fn set_static_analysis_off_reason(
        &mut self,
        static_analysis_off_reason: Option<StaticAnalysisOffReason>,
    ) {
        self.base_mut().static_analysis_off_reason = static_analysis_off_reason;
    }

    // Optional Fields
    fn get_access(&self) -> Option<Access> {
        None
    }
    fn get_group(&self) -> Option<String> {
        None
    }

    /// Returns the search name for this node, following Python dbt patterns:
    /// - Models: name (or name.v{version} if versioned)
    /// - Sources: source_name.name
    /// - Others: name
    fn search_name(&self) -> String;

    /// Returns the selector string for this node, following Python dbt patterns:
    /// - Models/Seeds/Tests/Snapshots: use fqn joined with "."
    /// - Sources: "source:pkg.source_name.table_name"
    /// - Unit tests: "unit_test:pkg.versioned_name"
    fn selector_string(&self) -> String;

    /// Returns the file path for this node
    fn file_path(&self) -> String {
        self.common()
            .original_file_path
            .to_string_lossy()
            .to_string()
    }

    // TO BE DEPRECATED
    fn serialized_config(&self) -> YmlValue;
}

// Shared helper functions for has_same_content implementation across node types
// These match the dbt-core same_contents method logic for ParsedNode

fn same_body(self_common: &CommonAttributes, other_common: &CommonAttributes) -> bool {
    self_common.checksum == other_common.checksum
}

fn same_persisted_description(
    self_common: &CommonAttributes,
    self_base: &NodeBaseAttributes,
    other_common: &CommonAttributes,
    other_base: &NodeBaseAttributes,
) -> bool {
    // If persist_docs settings differ, they're not the same
    if !persist_docs_configs_equal(&self_base.persist_docs, &other_base.persist_docs) {
        return false;
    }

    // Extract the persist settings for use below
    let self_persist_relation = self_base
        .persist_docs
        .as_ref()
        .and_then(|pd| pd.relation)
        .unwrap_or(false);

    let self_persist_columns = self_base
        .persist_docs
        .as_ref()
        .and_then(|pd| pd.columns)
        .unwrap_or(false);

    // Helper function to normalize descriptions: treat None and Some("") as equal
    // and trim leading/trailing newlines for non-empty descriptions
    fn normalize_description(desc: &Option<String>) -> Option<String> {
        desc.as_deref()
            .filter(|s| !s.is_empty())
            .map(|s| s.trim_matches('\n').to_string())
    }

    // If relation docs are persisted, compare descriptions
    if self_persist_relation
        && normalize_description(&self_common.description)
            != normalize_description(&other_common.description)
    {
        return false;
    }

    // If column docs are persisted, compare column descriptions
    if self_persist_columns {
        let self_column_descriptions: Vec<_> = self_base
            .columns
            .iter()
            .map(|column| column.description.clone())
            .collect();

        let other_column_descriptions: Vec<_> = other_base
            .columns
            .iter()
            .map(|column| column.description.clone())
            .collect();

        if self_column_descriptions != other_column_descriptions {
            return false;
        }
    }

    true
}

fn same_fqn(self_common: &CommonAttributes, other_common: &CommonAttributes) -> bool {
    let self_fqn = &self_common.fqn;
    let other_fqn = &other_common.fqn;

    // If they're exactly equal, return true
    if self_fqn == other_fqn {
        return true;
    }

    // Determine which is shorter and which is longer
    let (shorter, longer) = if self_fqn.len() <= other_fqn.len() {
        (self_fqn, other_fqn)
    } else {
        (other_fqn, self_fqn)
    };

    // Check if the shorter FQN matches a prefix of the longer FQN
    if longer.starts_with(shorter) {
        return true;
    }

    false
}

/// Compare the table names (last part after splitting by '.') from two relation_name Option values
pub fn same_relation_name(self_relation: &Option<String>, other_relation: &Option<String>) -> bool {
    match (self_relation, other_relation) {
        (None, None) => true,
        (None, Some(other)) => other.split('.').next_back() == Some(""),
        (Some(self_rel), None) => self_rel.split('.').next_back() == Some(""),
        (Some(self_rel), Some(other)) => {
            self_rel.split('.').next_back() == other.split('.').next_back()
        }
    }
}

// Helper function to compare PersistDocsConfig values
fn persist_docs_configs_equal(
    a: &Option<PersistDocsConfig>,
    b: &Option<PersistDocsConfig>,
) -> bool {
    // Helper to check if a PersistDocsConfig has all fields as None
    let is_empty_persist_docs =
        |pd: &PersistDocsConfig| -> bool { pd.columns.is_none() && pd.relation.is_none() };

    // First handle the special case where None equals Some with all None fields
    match (a, b) {
        (None, None) => return true,
        (None, Some(b_config)) if is_empty_persist_docs(b_config) => return true,
        (Some(a_config), None) if is_empty_persist_docs(a_config) => return true,
        _ => {}
    }

    // Extract values for comparison
    let a_persist_relation = a.as_ref().and_then(|pd| pd.relation).unwrap_or(false);
    let b_persist_relation = b.as_ref().and_then(|pd| pd.relation).unwrap_or(false);

    let a_persist_columns = a.as_ref().and_then(|pd| pd.columns).unwrap_or(false);
    let b_persist_columns = b.as_ref().and_then(|pd| pd.columns).unwrap_or(false);

    // Both relation and columns settings must match
    a_persist_relation == b_persist_relation && a_persist_columns == b_persist_columns
}

fn same_database_representation_model(
    self_config: &ModelConfig,
    other_config: &ModelConfig,
) -> bool {
    // Compare database, schema, alias from the deprecated_config
    self_config.same_database_representation(other_config)
}

fn same_database_representation_seed(self_config: &SeedConfig, other_config: &SeedConfig) -> bool {
    // Compare database, schema, alias from the deprecated_config
    /*self_config.database == other_config.database
    && self_config.schema == other_config.schema*/
    self_config.alias == other_config.alias
}

fn same_database_representation_snapshot(
    self_config: &SnapshotConfig,
    other_config: &SnapshotConfig,
) -> bool {
    // Compare database, schema, alias from the deprecated_config
    /*self_config.database == other_config.database
    && self_config.schema == other_config.schema*/
    self_config.alias == other_config.alias
}

impl InternalDbtNode for DbtModel {
    fn common(&self) -> &CommonAttributes {
        &self.__common_attr__
    }

    fn base(&self) -> &NodeBaseAttributes {
        &self.__base_attr__
    }

    fn version(&self) -> Option<StringOrInteger> {
        self.__model_attr__.version.clone()
    }

    fn latest_version(&self) -> Option<StringOrInteger> {
        self.__model_attr__.latest_version.clone()
    }

    fn event_time(&self) -> Option<String> {
        self.__model_attr__.event_time.clone()
    }

    fn is_versioned(&self) -> bool {
        self.__model_attr__.version.is_some()
    }

    fn is_extended_model(&self) -> bool {
        self.__base_attr__.extended_model
    }

    fn resource_type(&self) -> NodeType {
        NodeType::Model
    }

    fn base_mut(&mut self) -> &mut NodeBaseAttributes {
        &mut self.__base_attr__
    }

    fn common_mut(&mut self) -> &mut CommonAttributes {
        &mut self.__common_attr__
    }
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn serialize_inner(
        &self,
        mode: crate::schemas::serialization_utils::SerializationMode,
    ) -> YmlValue {
        crate::schemas::serialization_utils::serialize_with_mode(self, mode)
    }

    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_model) = other.as_any().downcast_ref::<DbtModel>() {
            self.deprecated_config
                .same_config(&other_model.deprecated_config)
        } else {
            false
        }
    }

    fn has_same_content(&self, other: &dyn InternalDbtNode) -> bool {
        // TODO: the checksum for extended model is always different in mantle and fusion, dig more into this
        if self.is_extended_model() {
            return true;
        }

        if let Some(other_model) = other.as_any().downcast_ref::<DbtModel>() {
            // Equivalent to dbt-core's same_contents method for ParsedNode
            same_body(&self.__common_attr__, &other_model.__common_attr__)
                && self.has_same_config(other)
                && same_persisted_description(
                    &self.__common_attr__,
                    &self.__base_attr__,
                    &other_model.__common_attr__,
                    &other_model.__base_attr__,
                )
                && same_fqn(&self.__common_attr__, &other_model.__common_attr__)
                && same_database_representation_model(
                    &self.deprecated_config,
                    &other_model.deprecated_config,
                )
                && self.same_contract(other_model)
        } else {
            false
        }
    }
    fn set_detected_introspection(&mut self, introspection: IntrospectionKind) {
        self.__model_attr__.introspection = introspection;
    }
    fn introspection(&self) -> IntrospectionKind {
        self.__model_attr__.introspection
    }

    fn warn_on_microbatch(&self) -> FsResult<()> {
        if let Some(DbtIncrementalStrategy::Microbatch) = self.__model_attr__.incremental_strategy {
            return err!(
                code => ErrorCode::UnsupportedFeature,
                loc => self.path(),
                "Microbatch incremental strategy is not supported. Use --exclude config.incremental_strategy:microbatch to exclude these models."
            );
        }
        Ok(())
    }
}

impl InternalDbtNodeAttributes for DbtModel {
    fn get_access(&self) -> Option<Access> {
        Some(self.__model_attr__.access.clone())
    }

    fn get_group(&self) -> Option<String> {
        self.__model_attr__.group.clone()
    }

    fn search_name(&self) -> String {
        if let Some(version) = &self.__model_attr__.version {
            format!("{}.v{}", self.__common_attr__.name, version)
        } else {
            self.__common_attr__.name.clone()
        }
    }

    fn selector_string(&self) -> String {
        self.__common_attr__.fqn.join(".")
    }

    fn serialized_config(&self) -> YmlValue {
        dbt_serde_yaml::to_value(&self.deprecated_config).expect("Failed to serialize to YAML")
    }
}

/// Helper function to compare materialized fields for seeds, treating None and default Seed materialization as equivalent
fn seed_materialized_eq(a: &Option<DbtMaterialization>, b: &Option<DbtMaterialization>) -> bool {
    use crate::schemas::common::DbtMaterialization;
    // Default value for seeds is always "seed"
    // See https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/artifacts/resources/v1/seed.py#L16
    let default_seed_materialized = DbtMaterialization::Seed;

    match (a, b) {
        // Both None
        (None, None) => true,
        // Both Some - direct comparison
        (Some(a_val), Some(b_val)) => a_val == b_val,
        // One None, one Some - check if the Some value equals default seed materialization
        (None, Some(b_val)) => b_val == &default_seed_materialized,
        (Some(a_val), None) => a_val == &default_seed_materialized,
    }
}

/// Helper functions for smart comparison of SeedConfig fields that considers
/// None vs Some(empty) representations as equivalent
fn seed_configs_equal(left: &SeedConfig, right: &SeedConfig) -> bool {
    // Compare each field with smart empty comparison
    btree_map_equal(&left.column_types, &right.column_types) &&
    docs_config_equal(&left.docs, &right.docs) &&
    left.enabled == right.enabled &&
    btree_map_string_or_array_equal(&left.grants, &right.grants) &&
    left.quote_columns == right.quote_columns &&
    // left.delimiter == right.delimiter && // TODO: re-enable when no longer using mantle/core manifests in IA
    left.event_time == right.event_time &&
    left.full_refresh == right.full_refresh &&
    btree_map_yml_value_equal(&left.meta, &right.meta) &&
    persist_docs_configs_equal(&left.persist_docs, &right.persist_docs) &&
    hooks_equal(&left.post_hook, &right.post_hook) &&
    hooks_equal(&left.pre_hook, &right.pre_hook) &&
    // quoting_equal(&left.quoting, &right.quoting) && // TODO: re-enable when no longer using mantle/core manifests in IA
    seed_materialized_eq(&left.materialized, &right.materialized) &&
    same_warehouse_config(&left.__warehouse_specific_config__, &right.__warehouse_specific_config__)
}

/// Compare BTreeMap<Spanned<String>, String> considering None vs Some(empty) as equal
fn btree_map_equal(
    left: &Option<BTreeMap<Spanned<String>, String>>,
    right: &Option<BTreeMap<Spanned<String>, String>>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(l), Some(r)) => {
            // Convert both maps to lowercase for case-insensitive comparison
            let l_normalized: BTreeMap<String, String> = l
                .iter()
                .map(|(k, v)| (k.to_lowercase(), v.to_lowercase()))
                .collect();
            let r_normalized: BTreeMap<String, String> = r
                .iter()
                .map(|(k, v)| (k.to_lowercase(), v.to_lowercase()))
                .collect();
            l_normalized == r_normalized
        }
        (None, Some(r)) => r.is_empty(),
        (Some(l), None) => l.is_empty(),
    }
}

/// Compare BTreeMap<String, StringOrArrayOfStrings> considering None vs Some(empty) as equal
fn btree_map_string_or_array_equal(
    left: &Option<BTreeMap<String, StringOrArrayOfStrings>>,
    right: &Option<BTreeMap<String, StringOrArrayOfStrings>>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(l), Some(r)) => l == r,
        (None, Some(r)) => r.is_empty(),
        (Some(l), None) => l.is_empty(),
    }
}

/// Compare BTreeMap<String, YmlValue> considering None vs Some(empty) as equal
fn btree_map_yml_value_equal(
    left: &Option<BTreeMap<String, YmlValue>>,
    right: &Option<BTreeMap<String, YmlValue>>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(l), Some(r)) => l == r,
        (None, Some(r)) => r.is_empty(),
        (Some(l), None) => l.is_empty(),
    }
}

/// Compare DocsConfig considering None vs Some(default) as equal
fn docs_config_equal(
    left: &Option<crate::schemas::common::DocsConfig>,
    right: &Option<crate::schemas::common::DocsConfig>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(l), Some(r)) => l == r,
        (
            None,
            Some(crate::schemas::common::DocsConfig {
                show: true,
                node_color: None,
            }),
        ) => true,
        (
            Some(crate::schemas::common::DocsConfig {
                show: true,
                node_color: None,
            }),
            None,
        ) => true,
        _ => false,
    }
}

/// Compare DbtQuoting structures with more nuanced logic
/// This handles cases where one side has values and the other has all None fields
#[allow(dead_code)]
fn quoting_equal(
    left: &Option<crate::schemas::common::DbtQuoting>,
    right: &Option<crate::schemas::common::DbtQuoting>,
) -> bool {
    use crate::schemas::common::DbtQuoting;

    // Helper to check if a DbtQuoting has all fields as None
    let is_all_none = |q: &DbtQuoting| -> bool {
        q.database.is_none()
            && q.identifier.is_none()
            && q.schema.is_none()
            && q.snowflake_ignore_case.is_none()
    };

    // Helper to check if a DbtQuoting has all fields as Some(false)
    let is_all_false = |q: &DbtQuoting| -> bool {
        q.database == Some(false)
            && q.identifier == Some(false)
            && q.schema == Some(false)
            && q.snowflake_ignore_case == Some(false)
    };

    // Helper to check if a DbtQuoting is effectively "default" (all None or all false)
    let is_default = |q: &DbtQuoting| -> bool { is_all_none(q) || is_all_false(q) };

    match (left, right) {
        (None, None) => true,
        (Some(l), Some(r)) => {
            // Direct equality check first
            if l == r {
                return true;
            }

            // Check if both are effectively default
            if is_default(l) && is_default(r) {
                return true;
            }

            // Compare field by field, treating None and Some(false) as equal
            let database_eq = match (l.database, r.database) {
                (None, None) | (Some(false), Some(false)) => true,
                (None, Some(false)) | (Some(false), None) => true,
                (Some(a), Some(b)) => a == b,
                _ => false,
            };

            let identifier_eq = match (l.identifier, r.identifier) {
                (None, None) | (Some(false), Some(false)) => true,
                (None, Some(false)) | (Some(false), None) => true,
                (Some(a), Some(b)) => a == b,
                _ => false,
            };

            let schema_eq = match (l.schema, r.schema) {
                (None, None) | (Some(false), Some(false)) => true,
                (None, Some(false)) | (Some(false), None) => true,
                (Some(a), Some(b)) => a == b,
                _ => false,
            };

            let snowflake_ignore_case_eq = match (l.snowflake_ignore_case, r.snowflake_ignore_case)
            {
                (None, None) | (Some(false), Some(false)) => true,
                (None, Some(false)) | (Some(false), None) => true,
                (Some(a), Some(b)) => a == b,
                _ => false,
            };

            database_eq && identifier_eq && schema_eq && snowflake_ignore_case_eq
        }
        (None, Some(r)) => is_default(r),
        (Some(l), None) => is_default(l),
    }
}

impl InternalDbtNode for DbtSeed {
    fn resource_type(&self) -> NodeType {
        NodeType::Seed
    }

    fn common(&self) -> &CommonAttributes {
        &self.__common_attr__
    }

    fn common_mut(&mut self) -> &mut CommonAttributes {
        &mut self.__common_attr__
    }

    fn base(&self) -> &NodeBaseAttributes {
        &self.__base_attr__
    }
    fn base_mut(&mut self) -> &mut NodeBaseAttributes {
        &mut self.__base_attr__
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn serialize_inner(
        &self,
        mode: crate::schemas::serialization_utils::SerializationMode,
    ) -> YmlValue {
        crate::schemas::serialization_utils::serialize_with_mode(self, mode)
    }

    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_model) = other.as_any().downcast_ref::<DbtSeed>() {
            seed_configs_equal(&self.deprecated_config, &other_model.deprecated_config)
        } else {
            false
        }
    }

    fn has_same_content(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_seed) = other.as_any().downcast_ref::<DbtSeed>() {
            // Equivalent to dbt-core's same_contents method for ParsedNode
            // TODO: Seeds might have path based checksum. When they do,
            // warnings are logged about this. Implement these warnings later
            // after confirming they make sense. See:
            //https://github.com/dbt-labs/dbt-core/blob/b75d5e701ef4dc2d7a98c5301ef63ecfc02eae15/core/dbt/contracts/graph/nodes.py#L900-L933
            same_body(&self.__common_attr__, &other_seed.__common_attr__)
                && self.has_same_config(other)
                && same_persisted_description(
                    &self.__common_attr__,
                    &self.__base_attr__,
                    &other_seed.__common_attr__,
                    &other_seed.__base_attr__,
                )
                && same_fqn(&self.__common_attr__, &other_seed.__common_attr__)
                && same_database_representation_seed(
                    &self.deprecated_config,
                    &other_seed.deprecated_config,
                )
            // For seeds, same_contract always returns true in dbt-core
            // Seeds don't have complex contract validation like models,
            // so we do not need to do a contract check for seeds.
        } else {
            false
        }
    }
    fn set_detected_introspection(&mut self, _introspection: IntrospectionKind) {
        panic!("DbtSeed does not support setting detected_unsafe");
    }
}

impl InternalDbtNodeAttributes for DbtSeed {
    fn set_static_analysis(&mut self, _static_analysis: Spanned<StaticAnalysisKind>) {
        unimplemented!("static analysis metadata setting for schema nodes")
    }

    fn search_name(&self) -> String {
        self.__common_attr__.name.clone()
    }

    fn selector_string(&self) -> String {
        self.__common_attr__.fqn.join(".")
    }

    fn serialized_config(&self) -> YmlValue {
        dbt_serde_yaml::to_value(&self.deprecated_config).expect("Failed to serialize to YAML")
    }
}

impl InternalDbtNode for DbtTest {
    fn common(&self) -> &CommonAttributes {
        &self.__common_attr__
    }

    fn base(&self) -> &NodeBaseAttributes {
        &self.__base_attr__
    }

    fn resource_type(&self) -> NodeType {
        NodeType::Test
    }

    fn base_mut(&mut self) -> &mut NodeBaseAttributes {
        &mut self.__base_attr__
    }

    fn common_mut(&mut self) -> &mut CommonAttributes {
        &mut self.__common_attr__
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn defined_at(&self) -> Option<&dbt_common::CodeLocation> {
        self.defined_at.as_ref()
    }

    fn serialize_inner(
        &self,
        mode: crate::schemas::serialization_utils::SerializationMode,
    ) -> YmlValue {
        crate::schemas::serialization_utils::serialize_with_mode(self, mode)
    }

    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other) = other.as_any().downcast_ref::<DbtTest>() {
            // these fields are what dbt compares for test nodes
            // Some other configs were skipped
            // The dbt-core method compares unrendered_config values for
            // database as well, but we do not have unrendered_config values
            // so we do not do that comparison
            self.deprecated_config.enabled == other.deprecated_config.enabled
                && self.deprecated_config.alias == other.deprecated_config.alias
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
            self.has_same_config(other) && self.common().fqn == other.common().fqn
        } else {
            false
        }
    }
    fn set_detected_introspection(&mut self, introspection: IntrospectionKind) {
        self.__test_attr__.introspection = introspection;
    }
    fn introspection(&self) -> IntrospectionKind {
        self.__test_attr__.introspection
    }
}

impl InternalDbtNodeAttributes for DbtTest {
    fn search_name(&self) -> String {
        self.__common_attr__.name.clone()
    }

    fn selector_string(&self) -> String {
        self.__common_attr__.fqn.join(".")
    }

    fn serialized_config(&self) -> YmlValue {
        dbt_serde_yaml::to_value(&self.deprecated_config).expect("Failed to serialize to YAML")
    }
}

impl InternalDbtNode for DbtUnitTest {
    fn common(&self) -> &CommonAttributes {
        &self.__common_attr__
    }

    fn base(&self) -> &NodeBaseAttributes {
        &self.__base_attr__
    }

    fn resource_type(&self) -> NodeType {
        NodeType::UnitTest
    }

    fn base_mut(&mut self) -> &mut NodeBaseAttributes {
        &mut self.__base_attr__
    }

    fn common_mut(&mut self) -> &mut CommonAttributes {
        &mut self.__common_attr__
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn serialize_inner(
        &self,
        mode: crate::schemas::serialization_utils::SerializationMode,
    ) -> YmlValue {
        crate::schemas::serialization_utils::serialize_with_mode(self, mode)
    }

    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other) = other.as_any().downcast_ref::<DbtUnitTest>() {
            let self_config = &self.deprecated_config;
            let other_config = &other.deprecated_config;

            self_config.enabled == other_config.enabled
                && self_config.static_analysis == other_config.static_analysis
                && same_warehouse_config(
                    &self_config.__warehouse_specific_config__,
                    &other_config.__warehouse_specific_config__,
                )
        } else {
            false
        }
    }

    fn has_same_content(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other) = other.as_any().downcast_ref::<DbtUnitTest>() {
            let same_fqn = self.common().fqn == other.common().fqn;
            if !same_fqn {
                println!(
                    "FQN differs: self={:?}, other={:?}",
                    self.common().fqn,
                    other.common().fqn
                );
            }
            same_fqn
        } else {
            false
        }
    }
    fn set_detected_introspection(&mut self, _introspection: IntrospectionKind) {
        panic!("DbtUnitTest does not support setting detected_unsafe");
    }
}

impl InternalDbtNodeAttributes for DbtUnitTest {
    fn search_name(&self) -> String {
        // Based on Python implementation, unit tests can have a versioned name
        if let Some(version) = &self.__unit_test_attr__.version {
            format!("{}_v{}", self.__common_attr__.name, version)
        } else {
            self.__common_attr__.name.clone()
        }
    }

    fn selector_string(&self) -> String {
        format!(
            "unit_test:{}.{}",
            self.__common_attr__.package_name,
            self.search_name()
        )
    }

    fn serialized_config(&self) -> YmlValue {
        dbt_serde_yaml::to_value(&self.deprecated_config).expect("Failed to serialize to YAML")
    }
}

impl InternalDbtNode for DbtSource {
    fn common(&self) -> &CommonAttributes {
        &self.__common_attr__
    }

    fn base(&self) -> &NodeBaseAttributes {
        &self.__base_attr__
    }

    fn resource_type(&self) -> NodeType {
        NodeType::Source
    }

    fn event_time(&self) -> Option<String> {
        self.deprecated_config.event_time.clone()
    }

    fn base_mut(&mut self) -> &mut NodeBaseAttributes {
        &mut self.__base_attr__
    }

    fn common_mut(&mut self) -> &mut CommonAttributes {
        &mut self.__common_attr__
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn serialize_inner(
        &self,
        mode: crate::schemas::serialization_utils::SerializationMode,
    ) -> YmlValue {
        crate::schemas::serialization_utils::serialize_with_mode(self, mode)
    }

    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_source) = other.as_any().downcast_ref::<DbtSource>() {
            // Merged fields are scattered across the DbtSource struct, while unmerged
            // fields are in deprecated_config.
            let self_config = &self.deprecated_config;
            let other_config = &other_source.deprecated_config;

            // Helper function to compare loaded_at_query where None equals Some("")
            let loaded_at_eq = |a: &Option<String>, b: &Option<String>| -> bool {
                match (a, b) {
                    (None, None) => true,
                    (None, Some(b_val)) => b_val.is_empty(),
                    (Some(a_val), None) => a_val.is_empty(),
                    (Some(a_val), Some(b_val)) => a_val == b_val,
                }
            };
            // Helper function to compare quoting where None equals default DbtQuoting
            let quoting_eq = |a: &Option<crate::schemas::common::DbtQuoting>,
                              b: &Option<crate::schemas::common::DbtQuoting>|
             -> bool {
                let default_quoting = crate::schemas::common::DbtQuoting {
                    database: Some(false),
                    identifier: Some(false),
                    schema: Some(false),
                    snowflake_ignore_case: Some(false),
                };
                match (a, b) {
                    (None, None) => true,
                    (None, Some(b_val)) => b_val == &default_quoting,
                    (Some(a_val), None) => a_val == &default_quoting,
                    (Some(a_val), Some(b_val)) => a_val == b_val,
                }
            };
            // Compare each field individually
            let enabled_eq = self.__base_attr__.enabled == other_source.__base_attr__.enabled;

            let event_time_eq = self_config.event_time == other_config.event_time;

            // Helper function to compare freshness where None equals FreshnessDefinition with empty rules
            let freshness_eq = {
                use crate::schemas::common::FreshnessDefinition;

                // Check if a FreshnessDefinition is essentially empty (all fields are None)
                let is_empty_freshness = |f: &FreshnessDefinition| -> bool {
                    match (&f.error_after, &f.warn_after, &f.filter) {
                        (Some(error), Some(warn), None) => {
                            error.count.is_none()
                                && error.period.is_none()
                                && warn.count.is_none()
                                && warn.period.is_none()
                        }
                        (None, None, None) => true,
                        _ => false,
                    }
                };

                match (
                    &self.__source_attr__.freshness,
                    &other_source.__source_attr__.freshness,
                ) {
                    (None, None) => true,
                    (None, Some(b_val)) => is_empty_freshness(b_val),
                    (Some(a_val), None) => is_empty_freshness(a_val),
                    (Some(a_val), Some(b_val)) => {
                        let error_after_eq = match (&a_val.error_after, &b_val.error_after) {
                            (None, None) => true,
                            (None, Some(b_val)) => b_val.is_empty(),
                            (Some(a_val), None) => a_val.is_empty(),
                            (Some(a_val), Some(b_val)) => a_val == b_val,
                        };
                        let warn_after_eq = match (&a_val.warn_after, &b_val.warn_after) {
                            (None, None) => true,
                            (None, Some(b_val)) => b_val.is_empty(),
                            (Some(a_val), None) => a_val.is_empty(),
                            (Some(a_val), Some(b_val)) => a_val == b_val,
                        };
                        let filter_eq = match (&a_val.filter, &b_val.filter) {
                            (None, None) => true,
                            (None, Some(b_val)) => b_val.is_empty(),
                            (Some(a_val), None) => a_val.is_empty(),
                            (Some(a_val), Some(b_val)) => a_val == b_val,
                        };

                        error_after_eq && warn_after_eq && filter_eq
                    }
                }
            };

            let quoting_eq = quoting_eq(&self_config.quoting, &other_config.quoting);

            let loaded_at_field_eq = loaded_at_eq(
                &self.__source_attr__.loaded_at_field,
                &other_source.__source_attr__.loaded_at_field,
            );

            let loaded_at_query_result = loaded_at_eq(
                &self.__source_attr__.loaded_at_query,
                &other_source.__source_attr__.loaded_at_query,
            );

            let static_analysis_eq = self_config.static_analysis == other_config.static_analysis;

            let warehouse_config_eq = same_warehouse_config(
                &self_config.__warehouse_specific_config__,
                &other_config.__warehouse_specific_config__,
            );

            enabled_eq
                && event_time_eq
                && freshness_eq
                && quoting_eq
                && loaded_at_field_eq
                && loaded_at_query_result
                && static_analysis_eq
                && warehouse_config_eq
        } else {
            false
        }
    }

    fn has_same_content(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_source) = other.as_any().downcast_ref::<DbtSource>() {
            same_relation_name(
                &self.__base_attr__.relation_name,
                &other_source.__base_attr__.relation_name,
            ) && self.__common_attr__.fqn == other_source.__common_attr__.fqn
                && self.has_same_config(other)
                && self.__source_attr__.loader == other_source.__source_attr__.loader
        } else {
            false
        }
    }
    fn set_detected_introspection(&mut self, _introspection: IntrospectionKind) {
        panic!("DbtSource does not support setting detected_unsafe");
    }
}

impl InternalDbtNodeAttributes for DbtSource {
    fn search_name(&self) -> String {
        format!(
            "{}.{}",
            self.__source_attr__.source_name, self.__common_attr__.name
        )
    }

    fn selector_string(&self) -> String {
        format!(
            "source:{}.{}.{}",
            self.__common_attr__.package_name,
            self.__source_attr__.source_name,
            self.__common_attr__.name
        )
    }

    fn serialized_config(&self) -> YmlValue {
        dbt_serde_yaml::to_value(&self.deprecated_config).expect("Failed to serialize DbtModel")
    }
}

impl InternalDbtNode for DbtSnapshot {
    fn common(&self) -> &CommonAttributes {
        &self.__common_attr__
    }

    fn base(&self) -> &NodeBaseAttributes {
        &self.__base_attr__
    }

    fn resource_type(&self) -> NodeType {
        NodeType::Snapshot
    }

    fn base_mut(&mut self) -> &mut NodeBaseAttributes {
        &mut self.__base_attr__
    }

    fn common_mut(&mut self) -> &mut CommonAttributes {
        &mut self.__common_attr__
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn serialize_inner(
        &self,
        mode: crate::schemas::serialization_utils::SerializationMode,
    ) -> YmlValue {
        crate::schemas::serialization_utils::serialize_with_mode(self, mode)
    }

    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_snapshot) = other.as_any().downcast_ref::<DbtSnapshot>() {
            let self_config = &self.deprecated_config;
            let other_config = &other_snapshot.deprecated_config;

            // Snapshot-specific Configuration
            let alias_eq = self_config.alias == other_config.alias;
            let materialized_eq = self_config.materialized == other_config.materialized;
            let strategy_eq = self_config.strategy == other_config.strategy;
            let unique_key_eq = self_config.unique_key == other_config.unique_key;
            let check_cols_eq = self_config.check_cols == other_config.check_cols;
            let updated_at_eq = self_config.updated_at == other_config.updated_at;
            let dbt_valid_to_current_eq =
                self_config.dbt_valid_to_current == other_config.dbt_valid_to_current;

            let snapshot_meta_column_names_eq = {
                use crate::schemas::project::configs::snapshot_config::SnapshotMetaColumnNames;

                // Helper to check if a SnapshotMetaColumnNames has all fields as None
                let is_empty_snapshot_meta = |meta: &SnapshotMetaColumnNames| -> bool {
                    meta.dbt_scd_id.is_none()
                        && meta.dbt_updated_at.is_none()
                        && meta.dbt_valid_from.is_none()
                        && meta.dbt_valid_to.is_none()
                        && meta.dbt_is_deleted.is_none()
                };

                match (
                    &self_config.snapshot_meta_column_names,
                    &other_config.snapshot_meta_column_names,
                ) {
                    (None, None) => true,
                    (None, Some(other_meta)) => is_empty_snapshot_meta(other_meta),
                    (Some(self_meta), None) => is_empty_snapshot_meta(self_meta),
                    (Some(self_meta), Some(other_meta)) => self_meta == other_meta,
                }
            };

            let hard_deletes_eq = self_config.hard_deletes == other_config.hard_deletes;
            let target_database_eq = self_config.target_database == other_config.target_database;
            let target_schema_eq = self_config.target_schema == other_config.target_schema;

            // General Configuration
            let enabled_eq = self_config.enabled == other_config.enabled;
            let pre_hook_eq = hooks_equal(&self_config.pre_hook, &other_config.pre_hook);
            let post_hook_eq = hooks_equal(&self_config.post_hook, &other_config.post_hook);
            let persist_docs_eq =
                persist_docs_configs_equal(&self_config.persist_docs, &other_config.persist_docs);
            let grants_eq =
                btree_map_string_or_array_equal(&self_config.grants, &other_config.grants);
            let event_time_eq = self_config.event_time == other_config.event_time;
            let quoting_eq = quoting_equal(&self_config.quoting, &other_config.quoting);
            let static_analysis_eq = self_config.static_analysis == other_config.static_analysis;
            let group_eq = self_config.group == other_config.group;
            let quote_columns_eq = self_config.quote_columns == other_config.quote_columns;
            let invalidate_hard_deletes_eq =
                self_config.invalidate_hard_deletes == other_config.invalidate_hard_deletes;

            // Adapter specific configs
            let warehouse_config_eq = same_warehouse_config(
                &self_config.__warehouse_specific_config__,
                &other_config.__warehouse_specific_config__,
            );

            alias_eq
                && materialized_eq
                && strategy_eq
                && unique_key_eq
                && check_cols_eq
                && updated_at_eq
                && dbt_valid_to_current_eq
                && snapshot_meta_column_names_eq
                && hard_deletes_eq
                && target_database_eq
                && target_schema_eq
                // General Configuration
                && enabled_eq
                && pre_hook_eq
                && post_hook_eq
                && persist_docs_eq
                && grants_eq
                && event_time_eq
                && quoting_eq
                && static_analysis_eq
                && group_eq
                && quote_columns_eq
                && invalidate_hard_deletes_eq
                // Adapter specific configs
                && warehouse_config_eq
        } else {
            false
        }
    }

    fn has_same_content(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_snapshot) = other.as_any().downcast_ref::<DbtSnapshot>() {
            // Equivalent to dbt-core's same_contents method for ParsedNode
            same_body(&self.__common_attr__, &other_snapshot.__common_attr__)
                && self.has_same_config(other)
                && same_persisted_description(
                    &self.__common_attr__,
                    &self.__base_attr__,
                    &other_snapshot.__common_attr__,
                    &other_snapshot.__base_attr__,
                )
                && same_fqn(&self.__common_attr__, &other_snapshot.__common_attr__)
                && same_database_representation_snapshot(
                    &self.deprecated_config,
                    &other_snapshot.deprecated_config,
                )
            // For snapshots, same_contract always returns true in dbt-core,
            // so we do not need to do a contract check for snapshots.
            // See: https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/contracts/graph/nodes.py#L374
        } else {
            false
        }
    }

    fn set_detected_introspection(&mut self, introspection: IntrospectionKind) {
        self.__snapshot_attr__.introspection = introspection;
    }

    fn introspection(&self) -> IntrospectionKind {
        self.__snapshot_attr__.introspection
    }
}

impl InternalDbtNodeAttributes for DbtSnapshot {
    fn skip_generate_schema_name_macro(&self) -> bool {
        self.deprecated_config.target_schema.is_some()
    }

    fn schema(&self) -> String {
        // prefer legacy config
        self.deprecated_config
            .target_schema
            .clone()
            .unwrap_or_else(|| self.base().schema.clone())
    }

    fn skip_generate_database_name_macro(&self) -> bool {
        self.deprecated_config.target_database.is_some()
    }

    fn database(&self) -> String {
        // prefer legacy config
        self.deprecated_config
            .target_database
            .clone()
            .unwrap_or_else(|| self.base().database.clone())
    }

    fn tags(&self) -> Vec<String> {
        self.__common_attr__.tags.clone()
    }

    fn meta(&self) -> BTreeMap<String, YmlValue> {
        self.__common_attr__.meta.clone()
    }

    fn search_name(&self) -> String {
        self.__common_attr__.name.clone()
    }

    fn selector_string(&self) -> String {
        self.__common_attr__.fqn.join(".")
    }

    fn serialized_config(&self) -> YmlValue {
        dbt_serde_yaml::to_value(&self.deprecated_config).expect("Failed to serialize to YAML")
    }
}

impl InternalDbtNode for DbtExposure {
    fn common(&self) -> &CommonAttributes {
        &self.__common_attr__
    }
    fn base(&self) -> &NodeBaseAttributes {
        &self.__base_attr__
    }
    fn base_mut(&mut self) -> &mut NodeBaseAttributes {
        &mut self.__base_attr__
    }
    fn common_mut(&mut self) -> &mut CommonAttributes {
        &mut self.__common_attr__
    }
    fn resource_type(&self) -> NodeType {
        NodeType::Exposure
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn serialize_inner(
        &self,
        mode: crate::schemas::serialization_utils::SerializationMode,
    ) -> YmlValue {
        crate::schemas::serialization_utils::serialize_with_mode(self, mode)
    }
    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_exposure) = other.as_any().downcast_ref::<DbtExposure>() {
            self.deprecated_config.enabled == other_exposure.deprecated_config.enabled
        } else {
            false
        }
    }
    fn has_same_content(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_exposure) = other.as_any().downcast_ref::<DbtExposure>() {
            self.__common_attr__.name == other_exposure.__common_attr__.name
                && self.__common_attr__.fqn == other_exposure.__common_attr__.fqn
                && self.__exposure_attr__.owner == other_exposure.__exposure_attr__.owner
                && self.__exposure_attr__.maturity == other_exposure.__exposure_attr__.maturity
                && self.__exposure_attr__.url == other_exposure.__exposure_attr__.url
                && self.__exposure_attr__.label == other_exposure.__exposure_attr__.label
        } else {
            false
        }
    }
    fn set_detected_introspection(&mut self, _introspection: IntrospectionKind) {
        panic!("DbtExposure does not support setting detected_unsafe");
    }
}

impl InternalDbtNodeAttributes for DbtExposure {
    fn materialized(&self) -> DbtMaterialization {
        self.__base_attr__.materialized.clone()
    }
    fn quoting(&self) -> ResolvedQuoting {
        self.__base_attr__.quoting
    }
    fn tags(&self) -> Vec<String> {
        self.__common_attr__.tags.clone()
    }
    fn meta(&self) -> BTreeMap<String, YmlValue> {
        self.__common_attr__.meta.clone()
    }
    fn serialized_config(&self) -> YmlValue {
        dbt_serde_yaml::to_value(&self.deprecated_config).expect("Failed to serialize to YAML")
    }
    fn search_name(&self) -> String {
        self.__common_attr__.name.clone()
    }
    fn selector_string(&self) -> String {
        self.__common_attr__.fqn.join(".")
    }
}

impl InternalDbtNode for DbtSemanticModel {
    fn common(&self) -> &CommonAttributes {
        &self.__common_attr__
    }
    fn base(&self) -> &NodeBaseAttributes {
        &self.__base_attr__
    }
    fn base_mut(&mut self) -> &mut NodeBaseAttributes {
        &mut self.__base_attr__
    }
    fn common_mut(&mut self) -> &mut CommonAttributes {
        &mut self.__common_attr__
    }
    fn resource_type(&self) -> NodeType {
        NodeType::SemanticModel
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn serialize_inner(
        &self,
        mode: crate::schemas::serialization_utils::SerializationMode,
    ) -> YmlValue {
        crate::schemas::serialization_utils::serialize_with_mode(self, mode)
    }
    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(_other_semantic_model) = other.as_any().downcast_ref::<DbtSemanticModel>() {
            // TODO: implement proper config comparison when needed
            true
        } else {
            false
        }
    }
    // This function only compares a subset of the DbSemanticModel node, similar to what
    // dbt-core does in SemanticModel.same_contents(). See:
    // https://github.com/dbt-labs/dbt-core/blob/906e07c1f2161aaf8873f17ba323221a3cf48c9f/core/dbt/contracts/graph/nodes.py#L1585-L1602
    // TODO: group is not compared while it is in dbt-core. SemanticModel group is not implemented in dbt-fusion.
    fn has_same_content(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_semantic_model) = other.as_any().downcast_ref::<DbtSemanticModel>() {
            self.has_same_config(other)
                && self.__semantic_model_attr__.model
                    == other_semantic_model.__semantic_model_attr__.model
                && self.__common_attr__.description
                    == other_semantic_model.__common_attr__.description
                && self.__semantic_model_attr__.entities
                    == other_semantic_model.__semantic_model_attr__.entities
                && self.__semantic_model_attr__.dimensions
                    == other_semantic_model.__semantic_model_attr__.dimensions
                && self.__semantic_model_attr__.measures
                    == other_semantic_model.__semantic_model_attr__.measures
                && self.deprecated_config == other_semantic_model.deprecated_config
                && self.__semantic_model_attr__.primary_entity
                    == other_semantic_model.__semantic_model_attr__.primary_entity
        } else {
            false
        }
    }
    fn set_detected_introspection(&mut self, _introspection: IntrospectionKind) {
        panic!("DbtSemanticModel does not support setting detected_unsafe");
    }
}

impl InternalDbtNodeAttributes for DbtSemanticModel {
    fn set_quoting(&mut self, _quoting: ResolvedQuoting) {
        unimplemented!("")
    }
    fn set_static_analysis(&mut self, _static_analysis: Spanned<StaticAnalysisKind>) {
        unimplemented!("")
    }
    fn search_name(&self) -> String {
        self.name()
    }
    fn selector_string(&self) -> String {
        format!(
            "semantic_model:{}.{}",
            self.package_name(),
            self.search_name()
        )
    }
    fn serialized_config(&self) -> YmlValue {
        dbt_serde_yaml::to_value(&self.deprecated_config).expect("Failed to serialize to YAML")
    }
}

impl InternalDbtNode for DbtMetric {
    fn common(&self) -> &CommonAttributes {
        &self.__common_attr__
    }
    fn common_mut(&mut self) -> &mut CommonAttributes {
        &mut self.__common_attr__
    }
    fn base(&self) -> &NodeBaseAttributes {
        &self.__base_attr__
    }
    fn base_mut(&mut self) -> &mut NodeBaseAttributes {
        &mut self.__base_attr__
    }
    fn resource_type(&self) -> NodeType {
        NodeType::Metric
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn serialize_inner(
        &self,
        mode: crate::schemas::serialization_utils::SerializationMode,
    ) -> YmlValue {
        crate::schemas::serialization_utils::serialize_with_mode(self, mode)
    }
    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_metric) = other.as_any().downcast_ref::<DbtMetric>() {
            let self_config = &self.deprecated_config;
            let other_config = &other_metric.deprecated_config;

            self_config.enabled == other_config.enabled && self_config.group == other_config.group
        } else {
            false
        }
    }
    // This function only compares a subset of the DbMetric node, similar to what
    // dbt-core does in Metric.same_contents(). See:
    // https://github.com/dbt-labs/dbt-core/blob/906e07c1f2161aaf8873f17ba323221a3cf48c9f/core/dbt/contracts/graph/nodes.py#L1496-L1511
    fn has_same_content(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_metric) = other.as_any().downcast_ref::<DbtMetric>() {
            self.has_same_config(other)
                && self.__metric_attr__.filter == other_metric.__metric_attr__.filter
                && self.__metric_attr__.metadata == other_metric.__metric_attr__.metadata
                && self.__metric_attr__.type_params == other_metric.__metric_attr__.type_params
                && self.__common_attr__.description == other_metric.__common_attr__.description
                && self.__common_attr__.fqn == other_metric.__common_attr__.fqn
                && self.__metric_attr__.label == other_metric.__metric_attr__.label
        } else {
            false
        }
    }
    fn set_detected_introspection(&mut self, _introspection: IntrospectionKind) {
        panic!("DbtMetric does not support setting detected_unsafe");
    }
}

impl InternalDbtNodeAttributes for DbtMetric {
    fn set_quoting(&mut self, _quoting: ResolvedQuoting) {
        unimplemented!("")
    }
    fn set_static_analysis(&mut self, _static_analysis: Spanned<StaticAnalysisKind>) {
        unimplemented!("")
    }
    fn search_name(&self) -> String {
        self.name()
    }
    fn selector_string(&self) -> String {
        format!("metric:{}.{}", self.package_name(), self.search_name())
    }
    fn serialized_config(&self) -> YmlValue {
        dbt_serde_yaml::to_value(&self.deprecated_config).expect("Failed to serialize to YAML")
    }
}

impl InternalDbtNode for DbtSavedQuery {
    fn common(&self) -> &CommonAttributes {
        &self.__common_attr__
    }
    fn base(&self) -> &NodeBaseAttributes {
        &self.__base_attr__
    }
    fn common_mut(&mut self) -> &mut CommonAttributes {
        &mut self.__common_attr__
    }
    fn base_mut(&mut self) -> &mut NodeBaseAttributes {
        &mut self.__base_attr__
    }
    fn resource_type(&self) -> NodeType {
        NodeType::SavedQuery
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn serialize_inner(
        &self,
        mode: crate::schemas::serialization_utils::SerializationMode,
    ) -> YmlValue {
        crate::schemas::serialization_utils::serialize_with_mode(self, mode)
    }
    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_saved_query) = other.as_any().downcast_ref::<DbtSavedQuery>() {
            let self_config = &self.deprecated_config;
            let other_config = &other_saved_query.deprecated_config;

            self_config.cache == other_config.cache
                && self_config.enabled == other_config.enabled
                && self_config.export_as == other_config.export_as
                && self_config.schema == other_config.schema
                && self_config.group == other_config.group
        } else {
            false
        }
    }
    fn has_same_content(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_saved_query) = other.as_any().downcast_ref::<DbtSavedQuery>() {
            self.has_same_config(other)
                && self.__common_attr__.description == other_saved_query.__common_attr__.description
                && self.__common_attr__.fqn == other_saved_query.__common_attr__.fqn
                && self.__saved_query_attr__.label == other_saved_query.__saved_query_attr__.label
                && self.__common_attr__.tags == other_saved_query.__common_attr__.tags
                && self.__saved_query_attr__.exports
                    == other_saved_query.__saved_query_attr__.exports
                && self.__saved_query_attr__.query_params.group_by
                    == other_saved_query.__saved_query_attr__.query_params.group_by
                && self.__saved_query_attr__.query_params.where_
                    == other_saved_query.__saved_query_attr__.query_params.where_
        } else {
            false
        }
    }
    fn set_detected_introspection(&mut self, _introspection: IntrospectionKind) {
        panic!("DbtSavedQuery does not support setting detected_unsafe");
    }
}

impl InternalDbtNodeAttributes for DbtSavedQuery {
    fn set_quoting(&mut self, _quoting: ResolvedQuoting) {
        unimplemented!("")
    }
    fn set_static_analysis(&mut self, _static_analysis: Spanned<StaticAnalysisKind>) {
        unimplemented!("")
    }
    fn search_name(&self) -> String {
        self.name()
    }
    fn selector_string(&self) -> String {
        format!("saved_query:{}.{}", self.package_name(), self.search_name())
    }
    fn serialized_config(&self) -> YmlValue {
        dbt_serde_yaml::to_value(&self.deprecated_config).expect("Failed to serialize to YAML")
    }
}

impl InternalDbtNode for DbtFunction {
    fn common(&self) -> &CommonAttributes {
        &self.__common_attr__
    }

    fn base(&self) -> &NodeBaseAttributes {
        &self.__base_attr__
    }

    fn resource_type(&self) -> NodeType {
        NodeType::Function
    }

    fn base_mut(&mut self) -> &mut NodeBaseAttributes {
        &mut self.__base_attr__
    }

    fn common_mut(&mut self) -> &mut CommonAttributes {
        &mut self.__common_attr__
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn serialize_inner(
        &self,
        mode: crate::schemas::serialization_utils::SerializationMode,
    ) -> YmlValue {
        crate::schemas::serialization_utils::serialize_with_mode(self, mode)
    }

    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_function) = other.as_any().downcast_ref::<DbtFunction>() {
            self.deprecated_config
                .same_config(&other_function.deprecated_config)
        } else {
            false
        }
    }

    fn has_same_content(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_function) = other.as_any().downcast_ref::<DbtFunction>() {
            self.__common_attr__.checksum == other_function.__common_attr__.checksum
        } else {
            false
        }
    }

    fn set_detected_introspection(&mut self, _introspection: IntrospectionKind) {
        // Functions don't support introspection in the same way as models
        // This could be a no-op or we could add introspection support later
    }
}

impl InternalDbtNodeAttributes for DbtFunction {
    fn static_analysis(&self) -> Spanned<StaticAnalysisKind> {
        self.__base_attr__.static_analysis.clone()
    }

    fn set_quoting(&mut self, quoting: ResolvedQuoting) {
        self.__base_attr__.quoting = quoting;
    }

    fn set_static_analysis(&mut self, static_analysis: Spanned<StaticAnalysisKind>) {
        self.__base_attr__.static_analysis = static_analysis;
    }

    fn get_access(&self) -> Option<Access> {
        Some(self.__function_attr__.access.clone())
    }

    fn get_group(&self) -> Option<String> {
        self.__function_attr__.group.clone()
    }

    fn search_name(&self) -> String {
        self.__common_attr__.name.clone()
    }

    fn selector_string(&self) -> String {
        self.__common_attr__.fqn.join(".")
    }

    fn serialized_config(&self) -> YmlValue {
        dbt_serde_yaml::to_value(&self.deprecated_config).expect("Failed to serialize to YAML")
    }
}

impl InternalDbtNode for DbtMacro {
    fn common(&self) -> &CommonAttributes {
        unimplemented!("macro common attributes access")
    }
    fn base(&self) -> &NodeBaseAttributes {
        unimplemented!("macro base attributes access")
    }
    fn base_mut(&mut self) -> &mut NodeBaseAttributes {
        unimplemented!("macro base attributes mutation")
    }
    fn common_mut(&mut self) -> &mut CommonAttributes {
        unimplemented!("macro common attributes mutation")
    }
    fn resource_type(&self) -> NodeType {
        NodeType::Macro
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn serialize_inner(
        &self,
        mode: crate::schemas::serialization_utils::SerializationMode,
    ) -> YmlValue {
        crate::schemas::serialization_utils::serialize_with_mode(self, mode)
    }
    fn has_same_config(&self, _other: &dyn InternalDbtNode) -> bool {
        unimplemented!("macro config comparison")
    }
    fn has_same_content(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_macro) = other.as_any().downcast_ref::<DbtMacro>() {
            self.macro_sql == other_macro.macro_sql
        } else {
            false
        }
    }
    fn set_detected_introspection(&mut self, _introspection: IntrospectionKind) {
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
    pub analyses: BTreeMap<String, Arc<DbtAnalysis>>,
    pub exposures: BTreeMap<String, Arc<DbtExposure>>,
    pub semantic_models: BTreeMap<String, Arc<DbtSemanticModel>>,
    pub metrics: BTreeMap<String, Arc<DbtMetric>>,
    pub saved_queries: BTreeMap<String, Arc<DbtSavedQuery>>,
    pub groups: BTreeMap<String, Arc<DbtGroup>>,
    pub functions: BTreeMap<String, Arc<DbtFunction>>,
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
        let exposures = self
            .exposures
            .iter()
            .map(|(id, node)| (id.clone(), Arc::new((**node).clone())))
            .collect();
        let semantic_models = self
            .semantic_models
            .iter()
            .map(|(id, node)| (id.clone(), Arc::new((**node).clone())))
            .collect();
        let metrics = self
            .metrics
            .iter()
            .map(|(id, node)| (id.clone(), Arc::new((**node).clone())))
            .collect();
        let saved_queries = self
            .saved_queries
            .iter()
            .map(|(id, node)| (id.clone(), Arc::new((**node).clone())))
            .collect();
        let groups = self
            .groups
            .iter()
            .map(|(id, node)| (id.clone(), Arc::new((**node).clone())))
            .collect();
        let functions = self
            .functions
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
            exposures,
            semantic_models,
            metrics,
            saved_queries,
            groups,
            functions,
        }
    }

    /// Return only the keys of materializable nodes (this excludes exposures and semantic resources)
    pub fn materializable_keys(&self) -> impl Iterator<Item = &String> {
        self.models
            .keys()
            .chain(self.seeds.keys())
            .chain(self.tests.keys())
            .chain(self.unit_tests.keys())
            .chain(self.sources.keys())
            .chain(self.snapshots.keys())
            .chain(self.analyses.keys())
            .chain(self.exposures.keys())
            .chain(self.functions.keys())
            .chain(self.semantic_models.keys())
            .chain(self.metrics.keys())
            .chain(self.saved_queries.keys())
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
            .or_else(|| {
                self.exposures
                    .get(unique_id)
                    .map(|n| Arc::as_ref(n) as &dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.functions
                    .get(unique_id)
                    .map(|n| Arc::as_ref(n) as &dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.semantic_models
                    .get(unique_id)
                    .map(|n| Arc::as_ref(n) as &dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.metrics
                    .get(unique_id)
                    .map(|n| Arc::as_ref(n) as &dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.saved_queries
                    .get(unique_id)
                    .map(|n| Arc::as_ref(n) as &dyn InternalDbtNodeAttributes)
            })
    }

    pub fn get_node_owned(&self, unique_id: &str) -> Option<Arc<dyn InternalDbtNodeAttributes>> {
        self.models
            .get(unique_id)
            .map(|n| n.clone() as Arc<dyn InternalDbtNodeAttributes>)
            .or_else(|| {
                self.seeds
                    .get(unique_id)
                    .map(|n| n.clone() as Arc<dyn InternalDbtNodeAttributes>)
            })
            .or_else(|| {
                self.tests
                    .get(unique_id)
                    .map(|n| n.clone() as Arc<dyn InternalDbtNodeAttributes>)
            })
            .or_else(|| {
                self.unit_tests
                    .get(unique_id)
                    .map(|n| n.clone() as Arc<dyn InternalDbtNodeAttributes>)
            })
            .or_else(|| {
                self.sources
                    .get(unique_id)
                    .map(|n| n.clone() as Arc<dyn InternalDbtNodeAttributes>)
            })
            .or_else(|| {
                self.snapshots
                    .get(unique_id)
                    .map(|n| n.clone() as Arc<dyn InternalDbtNodeAttributes>)
            })
            .or_else(|| {
                self.analyses
                    .get(unique_id)
                    .map(|n| n.clone() as Arc<dyn InternalDbtNodeAttributes>)
            })
            .or_else(|| {
                self.exposures
                    .get(unique_id)
                    .map(|n| n.clone() as Arc<dyn InternalDbtNodeAttributes>)
            })
            .or_else(|| {
                self.functions
                    .get(unique_id)
                    .map(|n| n.clone() as Arc<dyn InternalDbtNodeAttributes>)
            })
            .or_else(|| {
                self.semantic_models
                    .get(unique_id)
                    .map(|n| n.clone() as Arc<dyn InternalDbtNodeAttributes>)
            })
            .or_else(|| {
                self.metrics
                    .get(unique_id)
                    .map(|n| n.clone() as Arc<dyn InternalDbtNodeAttributes>)
            })
            .or_else(|| {
                self.saved_queries
                    .get(unique_id)
                    .map(|n| n.clone() as Arc<dyn InternalDbtNodeAttributes>)
            })
    }

    /// Check if a node exists in the graph.
    /// Used with [`Nodes::materializable_keys`], so intent could be to only check for materializable nodes.
    /// TODO: Determine if this function should be updated to only check for materializable nodes.
    pub fn contains(&self, unique_id: &str) -> bool {
        self.models.contains_key(unique_id)
            || self.seeds.contains_key(unique_id)
            || self.tests.contains_key(unique_id)
            || self.unit_tests.contains_key(unique_id)
            || self.sources.contains_key(unique_id)
            || self.snapshots.contains_key(unique_id)
            || self.analyses.contains_key(unique_id)
            || self.exposures.contains_key(unique_id)
            || self.semantic_models.contains_key(unique_id)
            || self.metrics.contains_key(unique_id)
            || self.functions.contains_key(unique_id)
            || self.saved_queries.contains_key(unique_id)
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
            .chain(
                self.exposures
                    .iter()
                    .map(|(id, node)| (id, Arc::as_ref(node) as &dyn InternalDbtNodeAttributes)),
            )
            .chain(
                self.functions
                    .iter()
                    .map(|(id, node)| (id, Arc::as_ref(node) as &dyn InternalDbtNodeAttributes)),
            )
            .chain(
                self.semantic_models
                    .iter()
                    .map(|(id, node)| (id, Arc::as_ref(node) as &dyn InternalDbtNodeAttributes)),
            )
            .chain(
                self.metrics
                    .iter()
                    .map(|(id, node)| (id, Arc::as_ref(node) as &dyn InternalDbtNodeAttributes)),
            )
            .chain(
                self.saved_queries
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
        let exposures = self
            .exposures
            .iter()
            .map(|(id, node)| (id.clone(), upcast(node.clone())));
        let functions = self
            .functions
            .iter()
            .map(|(id, node)| (id.clone(), upcast(node.clone())));
        let semantic_models = self
            .semantic_models
            .iter()
            .map(|(id, node)| (id.clone(), upcast(node.clone())));
        let metrics = self
            .metrics
            .iter()
            .map(|(id, node)| (id.clone(), upcast(node.clone())));
        let saved_queries = self
            .saved_queries
            .iter()
            .map(|(id, node)| (id.clone(), upcast(node.clone())));

        models
            .chain(seeds)
            .chain(tests)
            .chain(unit_tests)
            .chain(sources)
            .chain(snapshots)
            .chain(analyses)
            .chain(exposures)
            .chain(functions)
            .chain(semantic_models)
            .chain(metrics)
            .chain(saved_queries)
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
        let map_exposures = self
            .exposures
            .values_mut()
            .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNodeAttributes);
        let map_functions = self
            .functions
            .values_mut()
            .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNodeAttributes);
        let map_semantic_models = self
            .semantic_models
            .values_mut()
            .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNodeAttributes);
        let map_metrics = self
            .metrics
            .values_mut()
            .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNodeAttributes);
        let map_saved_queries = self
            .saved_queries
            .values_mut()
            .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNodeAttributes);

        map_models
            .chain(map_seeds)
            .chain(map_tests)
            .chain(map_unit_tests)
            .chain(map_sources)
            .chain(map_snapshots)
            .chain(map_analyses)
            .chain(map_exposures)
            .chain(map_functions)
            .chain(map_semantic_models)
            .chain(map_metrics)
            .chain(map_saved_queries)
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
            .or_else(|| {
                self.exposures
                    .get_mut(unique_id)
                    .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.functions
                    .get_mut(unique_id)
                    .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.semantic_models
                    .get_mut(unique_id)
                    .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.metrics
                    .get_mut(unique_id)
                    .map(|arc| Arc::make_mut(arc) as &mut dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.saved_queries
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
                    // test.base().relation_name is always None
                    .find(|test| test.relation_name() == relation_name)
                    .map(|arc| Arc::as_ref(arc) as &dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.unit_tests
                    .values()
                    .find(|unit_test| unit_test.relation_name() == relation_name)
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
            .or_else(|| {
                self.exposures
                    .values()
                    .find(|exposure| {
                        exposure.base().relation_name == Some(relation_name.to_string())
                    })
                    .map(|arc| Arc::as_ref(arc) as &dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.functions
                    .values()
                    .find(|function| {
                        function.base().relation_name == Some(relation_name.to_string())
                    })
                    .map(|arc| Arc::as_ref(arc) as &dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.semantic_models
                    .values()
                    .find(|semantic_model| {
                        semantic_model.base().relation_name == Some(relation_name.to_string())
                    })
                    .map(|arc| Arc::as_ref(arc) as &dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.metrics
                    .values()
                    .find(|metric| metric.base().relation_name == Some(relation_name.to_string()))
                    .map(|arc| Arc::as_ref(arc) as &dyn InternalDbtNodeAttributes)
            })
            .or_else(|| {
                self.saved_queries
                    .values()
                    .find(|saved_query| {
                        saved_query.base().relation_name == Some(relation_name.to_string())
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
        self.exposures.extend(other.exposures);
        self.semantic_models.extend(other.semantic_models);
        self.metrics.extend(other.metrics);
        self.saved_queries.extend(other.saved_queries);
        self.groups.extend(other.groups);
        self.functions.extend(other.functions);
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
                    "  - {unique_id} (materialization: {materialization})\n"
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct CommonAttributes {
    // Identifiers
    pub unique_id: String,
    pub name: String,
    pub package_name: String,
    pub fqn: Vec<String>,

    // Paths
    pub path: PathBuf,

    /// The original file path where this node was defined
    ///
    /// **NOTE**: For [DbtTest] nodes, this is currently the path to the
    /// generated SQL file, *NOT* the path to the YAML file where the test was
    /// defined!
    pub original_file_path: PathBuf,

    pub raw_code: Option<String>,
    pub patch_path: Option<PathBuf>,
    pub name_span: dbt_common::Span,

    // Checksum
    pub checksum: DbtChecksum,
    pub language: Option<String>,

    // Meta
    pub description: Option<String>,

    // Tags and Meta
    pub tags: Vec<String>,
    pub meta: BTreeMap<String, YmlValue>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct NodeBaseAttributes {
    // Identifiers
    #[serde(default)]
    pub database: String,
    pub schema: String,
    pub alias: String,
    pub relation_name: Option<String>,

    // Resolved Quoting
    pub quoting: ResolvedQuoting,
    // TODO: Potentially add ignore_case to ResolvedQuoting
    pub quoting_ignore_case: bool,
    pub materialized: DbtMaterialization,
    pub static_analysis: Spanned<StaticAnalysisKind>,
    #[serde(skip_serializing, default)]
    pub static_analysis_off_reason: Option<StaticAnalysisOffReason>,
    pub enabled: bool,
    #[serde(skip_serializing, default = "default_false")]
    pub extended_model: bool,

    // Documentation persistence configuration
    pub persist_docs: Option<PersistDocsConfig>,

    // Derived
    #[serde(
        default,
        serialize_with = "serialize_dbt_columns",
        deserialize_with = "deserialize_dbt_columns"
    )]
    pub columns: Vec<DbtColumnRef>,

    // Raw Refs, Source, and Metric Dependencies from SQL
    #[serde(default)]
    pub refs: Vec<DbtRef>,
    #[serde(default)]
    pub sources: Vec<DbtSourceWrapper>,
    #[serde(default)]
    pub functions: Vec<DbtRef>,
    #[serde(default)]
    pub metrics: Vec<Vec<String>>,

    // Resolved Dependencies
    pub depends_on: NodeDependsOn,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtSeed {
    pub __common_attr__: CommonAttributes,

    pub __base_attr__: NodeBaseAttributes,

    pub __seed_attr__: DbtSeedAttr,

    // To be deprecated
    #[serde(rename = "config")]
    pub deprecated_config: SeedConfig,

    pub __other__: BTreeMap<String, YmlValue>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtSeedAttr {
    #[serde(default, skip_serializing_if = "is_false")]
    pub quote_columns: bool,
    pub column_types: Option<BTreeMap<Spanned<String>, String>>,
    pub delimiter: Option<String>,
    pub root_path: Option<PathBuf>,
}

fn is_false(b: &bool) -> bool {
    !b
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DbtExposure {
    pub __common_attr__: CommonAttributes,

    pub __base_attr__: NodeBaseAttributes,

    pub __exposure_attr__: DbtExposureAttr,

    // To be deprecated
    #[serde(rename = "config")]
    pub deprecated_config: ExposureConfig,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtExposureAttr {
    pub owner: DbtOwner,
    pub label: Option<String>,
    pub maturity: Option<String>,
    #[serde(rename = "type")]
    pub type_: String,
    pub url: Option<String>,
    pub unrendered_config: BTreeMap<String, YmlValue>,
    pub created_at: Option<f64>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtGroup {
    pub __common_attr__: CommonAttributes,
    pub __base_attr__: NodeBaseAttributes,
    pub __group_attr__: DbtGroupAttr,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtGroupAttr {
    pub owner: DbtOwner,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtUnitTest {
    pub __common_attr__: CommonAttributes,

    pub __base_attr__: NodeBaseAttributes,

    pub __unit_test_attr__: DbtUnitTestAttr,

    #[serde(rename = "_event_status")]
    pub field_event_status: Option<BTreeMap<String, YmlValue>>,
    #[serde(rename = "_pre_injected_sql")]
    pub field_pre_injected_sql: Option<String>,
    pub tested_node_unique_id: Option<String>,
    pub this_input_node_unique_id: Option<String>,

    // To be deprecated
    #[serde(rename = "config")]
    pub deprecated_config: UnitTestConfig,
}

impl DbtUnitTest {
    pub fn relation_name(&self) -> String {
        format!(
            "{}.{}.{}",
            self.__base_attr__.database, self.__base_attr__.schema, self.__base_attr__.alias
        )
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub struct DbtUnitTestAttr {
    pub model: String,
    pub given: Vec<Given>,
    pub expect: Expect,
    pub versions: Option<IncludeExclude>,
    pub version: Option<StringOrInteger>,
    pub overrides: Option<UnitTestOverrides>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtTest {
    pub __common_attr__: CommonAttributes,
    pub __base_attr__: NodeBaseAttributes,
    pub __test_attr__: DbtTestAttr,
    pub defined_at: Option<dbt_common::CodeLocation>,

    // not to be confused with __common_attr__.original_file_path, which is the path to the generated sql file
    pub manifest_original_file_path: PathBuf,

    // To be deprecated
    #[serde(rename = "config")]
    pub deprecated_config: DataTestConfig,

    pub __other__: BTreeMap<String, YmlValue>,
}

impl DbtTest {
    pub fn relation_name(&self) -> String {
        format!(
            "{}.{}.{}",
            self.__base_attr__.database, self.__base_attr__.schema, self.__base_attr__.alias
        )
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtTestAttr {
    pub column_name: Option<String>,
    pub attached_node: Option<String>,
    pub test_metadata: Option<TestMetadata>,
    pub file_key_name: Option<String>,
    #[serde(skip_serializing, default = "default_introspection")]
    pub introspection: IntrospectionKind,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct TestMetadata {
    pub name: String,
    pub kwargs: BTreeMap<String, YmlValue>,
    pub namespace: Option<String>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtSnapshot {
    pub __common_attr__: CommonAttributes,
    pub __base_attr__: NodeBaseAttributes,
    pub __snapshot_attr__: DbtSnapshotAttr,
    pub __adapter_attr__: AdapterAttr,

    /// To be deprecated
    #[serde(rename = "config")]
    pub deprecated_config: SnapshotConfig,
    // TODO: Deprecate compiled and compiled_code fields (This field is used by the materialization
    // macro when this node is passed into the jinja context)
    pub compiled: Option<bool>,
    pub compiled_code: Option<String>,

    pub __other__: BTreeMap<String, YmlValue>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtSnapshotAttr {
    pub snapshot_meta_column_names: SnapshotMetaColumnNames,
    #[serde(skip_serializing, default = "default_introspection")]
    pub introspection: IntrospectionKind,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtSource {
    pub __common_attr__: CommonAttributes,

    pub __base_attr__: NodeBaseAttributes,

    // Source Specific Attributes
    pub __source_attr__: DbtSourceAttr,

    // To be deprecated
    #[serde(rename = "config")]
    pub deprecated_config: SourceConfig,

    // Other untyped (or rather externally typed) keys that can be used by dbt packages.
    // For example, the `external` key is used by `dbt-external-tables`, but it's not
    // explicitly typed by dbt itself.
    // See: https://github.com/dbt-labs/dbt-external-tables
    pub __other__: BTreeMap<String, YmlValue>,
}
impl DbtSource {
    pub fn base(&self) -> &NodeBaseAttributes {
        &self.__base_attr__
    }
    pub fn common(&self) -> &CommonAttributes {
        &self.__common_attr__
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtSourceAttr {
    pub identifier: String,
    pub source_name: String,
    pub source_description: String,
    pub loader: String,
    pub loaded_at_field: Option<String>,
    pub loaded_at_query: Option<String>,
    #[serialize_always]
    pub freshness: Option<FreshnessDefinition>,
}

impl DbtSource {
    pub fn get_base_attr(&self) -> NodeBaseAttributes {
        self.__base_attr__.clone()
    }

    pub fn get_loaded_at_field(&self) -> &str {
        self.__source_attr__
            .loaded_at_field
            .as_ref()
            .map(AsRef::as_ref)
            .unwrap_or("")
    }

    pub fn get_loaded_at_query(&self) -> &str {
        self.__source_attr__
            .loaded_at_query
            .as_ref()
            .map(AsRef::as_ref)
            .unwrap_or("")
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtModel {
    pub __common_attr__: CommonAttributes,

    pub __base_attr__: NodeBaseAttributes,

    pub __model_attr__: DbtModelAttr,

    pub __adapter_attr__: AdapterAttr,

    // TO BE DEPRECATED
    #[serde(rename = "config")]
    pub deprecated_config: ModelConfig,

    pub __other__: BTreeMap<String, YmlValue>,
}

impl DbtModel {
    pub fn same_contract(&self, old: &DbtModel) -> bool {
        match (&self.__model_attr__.contract, &old.__model_attr__.contract) {
            (Some(current_contract), Some(old_contract)) => {
                self.same_contract_both_present(old, old_contract, current_contract)
            }
            (Some(_self_contract), None) => false,
            (None, Some(old_contract)) => self.same_contract_removed(old, old_contract),
            (None, None) => true,
        }
    }

    // If a previous state contract and current state contract are both present,
    // compare them for changes.
    fn same_contract_both_present(
        &self,
        old: &DbtModel,
        old_contract: &DbtContract,
        current_contract: &DbtContract,
    ) -> bool {
        // Contract was not previously enforced
        if !old_contract.enforced && !current_contract.enforced {
            // No change -- same_contract: True
            return true;
        }
        if !old_contract.enforced && current_contract.enforced {
            // Now it's enforced. This is a change, but not a breaking change -- same_contract: False
            return false;
        }

        // Otherwise: The contract was previously enforced, and we need to check for changes.
        // Happy path: The contract is still being enforced, and the checksums are identical.
        if current_contract.enforced && current_contract.checksum == old_contract.checksum {
            // No change -- same_contract: True
            return true;
        }

        // Otherwise: There has been a change.
        // We need to determine if it is a **breaking** change.
        // These are the categories of breaking changes:
        let mut contract_enforced_disabled: bool = false;
        let mut columns_removed: Vec<String> = Vec::new();
        let mut column_type_changes: Vec<BTreeMap<String, String>> = Vec::new();
        let mut enforced_column_constraint_removed: Vec<BTreeMap<String, YmlValue>> = Vec::new(); // column_name, constraint_type
        let mut enforced_model_constraint_removed: Vec<BTreeMap<String, YmlValue>> = Vec::new(); // constraint_type, columns
        let mut materialization_changed: Vec<String> = Vec::new();

        if old_contract.enforced && !current_contract.enforced {
            // Breaking change: the contract was previously enforced, and it no longer is
            contract_enforced_disabled = true;
        }
        let mut column_constraints_exist: bool = false;

        // Helper function to check if materialization enforces constraints
        let materialization_enforces_constraints = |mat: &DbtMaterialization| -> bool {
            matches!(
                mat,
                DbtMaterialization::Table | DbtMaterialization::Incremental
            )
        };

        // Next, compare each column from the previous contract (old.columns)
        for old_value in old.__base_attr__.columns.iter() {
            // Has this column been removed?
            if !self.__base_attr__.columns.contains(old_value) {
                columns_removed.push(old_value.name.clone());
            }
            // Has this column's data type changed?
            else if let Some(current_column) = self
                .__base_attr__
                .columns
                .iter()
                .find(|col| col == &old_value)
            {
                if old_value.data_type != current_column.data_type {
                    let mut type_change = BTreeMap::new();
                    type_change.insert("column_name".to_string(), old_value.name.clone());
                    type_change.insert(
                        "previous_column_type".to_string(),
                        old_value
                            .data_type
                            .clone()
                            .unwrap_or_else(|| "unknown".to_string()),
                    );
                    type_change.insert(
                        "current_column_type".to_string(),
                        current_column
                            .data_type
                            .clone()
                            .unwrap_or_else(|| "unknown".to_string()),
                    );
                    column_type_changes.push(type_change);
                }
            }

            // track if there are any column level constraints for the materialization check later
            if !old_value.constraints.is_empty() {
                column_constraints_exist = true;
            }

            // Have enforced columns level constraints changed?
            // Constraints are only enforced for table and incremental materializations.
            // We only really care if the old node was one of those materializations for breaking changes
            if let Some(current_column) = self
                .__base_attr__
                .columns
                .iter()
                .find(|col| col == &old_value)
            {
                if old_value.constraints != current_column.constraints
                    && materialization_enforces_constraints(&old.materialized())
                {
                    for old_constraint in &old_value.constraints {
                        if !current_column.constraints.contains(old_constraint) {
                            let mut constraint_removed = BTreeMap::new();
                            constraint_removed.insert(
                                "column_name".to_string(),
                                YmlValue::string(old_value.name.clone()),
                            );
                            constraint_removed.insert(
                                "constraint_name".to_string(),
                                YmlValue::string(old_constraint.name.clone().unwrap_or_default()),
                            );
                            constraint_removed.insert(
                                "constraint_type".to_string(),
                                YmlValue::string(format!("{:?}", old_constraint.type_)),
                            );
                            enforced_column_constraint_removed.push(constraint_removed);
                        }
                    }
                }
            }
        }

        // Now compare the model level constraints
        if old.__model_attr__.constraints != self.__model_attr__.constraints
            && materialization_enforces_constraints(&old.materialized())
        {
            for old_constraint in &old.__model_attr__.constraints {
                if !self.__model_attr__.constraints.contains(old_constraint) {
                    let mut constraint_removed = BTreeMap::new();
                    constraint_removed.insert(
                        "constraint_name".to_string(),
                        YmlValue::string(old_constraint.name.clone().unwrap_or_default()),
                    );
                    constraint_removed.insert(
                        "constraint_type".to_string(),
                        YmlValue::string(format!("{:?}", old_constraint.type_)),
                    );
                    enforced_model_constraint_removed.push(constraint_removed);
                }
            }
        }

        // Check for relevant materialization changes.
        if materialization_enforces_constraints(&old.materialized())
            && !materialization_enforces_constraints(&self.materialized())
            && (!old.__model_attr__.constraints.is_empty() || column_constraints_exist)
        {
            materialization_changed = vec![
                format!("{:?}", old.materialized()),
                format!("{:?}", self.materialized()),
            ];
        }

        // If a column has been added, it will be missing in the old.columns, and present in self.columns
        // That's a change (caught by the different checksums), but not a breaking change

        // Did we find any changes that we consider breaking? If there's an enforced contract, that's
        // a warning unless the model is versioned, then it's an error.
        if contract_enforced_disabled
            || !columns_removed.is_empty()
            || !column_type_changes.is_empty()
            || !enforced_model_constraint_removed.is_empty()
            || !enforced_column_constraint_removed.is_empty()
            || !materialization_changed.is_empty()
        {
            let mut breaking_changes = Vec::new();

            if contract_enforced_disabled {
                breaking_changes.push(
                        "Contract enforcement was removed: Previously, this model had an enforced contract. It is no longer configured to enforce its contract, and this is a breaking change.".to_string()
                    );
            }

            if !columns_removed.is_empty() {
                let columns_removed_str = columns_removed.join("\n    - ");
                breaking_changes.push(format!(
                    "Columns were removed: \n    - {columns_removed_str}"
                ));
            }

            if !column_type_changes.is_empty() {
                let column_type_changes_str = column_type_changes
                    .iter()
                    .map(|c| {
                        format!(
                            "{} ({} -> {})",
                            c.get("column_name")
                                .map(|s| s.as_str())
                                .unwrap_or("unknown"),
                            c.get("previous_column_type")
                                .map(|s| s.as_str())
                                .unwrap_or("unknown"),
                            c.get("current_column_type")
                                .map(|s| s.as_str())
                                .unwrap_or("unknown")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n    - ");
                breaking_changes.push(format!(
                    "Columns with data_type changes: \n    - {column_type_changes_str}"
                ));
            }

            if !enforced_column_constraint_removed.is_empty() {
                let column_constraint_changes_str = enforced_column_constraint_removed
                    .iter()
                    .map(|c| {
                        let constraint_name = c
                            .get("constraint_name")
                            .and_then(|v| v.as_str())
                            .unwrap_or_else(|| {
                                c.get("constraint_type")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("unknown")
                            });
                        let column_name = c
                            .get("column_name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown");
                        format!("'{constraint_name}' constraint on column {column_name}")
                    })
                    .collect::<Vec<_>>()
                    .join("\n    - ");
                breaking_changes.push(format!(
                        "Enforced column level constraints were removed: \n    - {column_constraint_changes_str}"
                    ));
            }

            if !enforced_model_constraint_removed.is_empty() {
                let model_constraint_changes_str = enforced_model_constraint_removed
                    .iter()
                    .map(|c| {
                        let constraint_name = c
                            .get("constraint_name")
                            .and_then(|v| v.as_str())
                            .unwrap_or_else(|| {
                                c.get("constraint_type")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("unknown")
                            });
                        let columns = c
                            .get("columns")
                            .and_then(|v| v.as_sequence())
                            .map(|seq| {
                                seq.iter()
                                    .filter_map(|v| v.as_str())
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            })
                            .unwrap_or_else(|| "unknown".to_string());
                        format!("'{constraint_name}' constraint on columns {columns}")
                    })
                    .collect::<Vec<_>>()
                    .join("\n    - ");
                breaking_changes.push(format!(
                        "Enforced model level constraints were removed: \n    - {model_constraint_changes_str}"
                    ));
            }

            if !materialization_changed.is_empty() {
                let materialization_changes_str = format!(
                    "{} -> {}",
                    materialization_changed
                        .first()
                        .map(|s| s.as_str())
                        .unwrap_or("unknown"),
                    materialization_changed
                        .get(1)
                        .map(|s| s.as_str())
                        .unwrap_or("unknown")
                );
                breaking_changes.push(format!(
                        "Materialization changed with enforced constraints: \n    - {materialization_changes_str}"
                    ));
            }

            // Generate warning or error depending on if the model is versioned
            let reasons = breaking_changes.join("\n  - ");
            if old.is_versioned() {
                self.log_contract_breaking_change_error(&reasons, &old.__common_attr__.name);
            } else {
                self.log_unversioned_breaking_change_warning(
                    &reasons,
                    &old.__common_attr__.name,
                    old.file_path(),
                );
            }
        }
        // Otherwise, the contract has changed -- same_contract: False
        false
    }
    fn same_contract_removed(&self, old: &DbtModel, old_contract: &DbtContract) -> bool {
        // If the contract wasn't previously enforced, no contract change has occurred
        if !old_contract.enforced {
            return true;
        }
        // Removed node is past its deprecation_date, so deletion does not constitute a contract change
        if let Some(deprecation_date_str) = &old.__model_attr__.deprecation_date {
            // Parse the deprecation date string using common formats
            if let Ok(deprecation_date) =
                chrono::NaiveDate::parse_from_str(deprecation_date_str, "%Y-%m-%d")
            {
                let deprecation_datetime = deprecation_date.and_hms_opt(0, 0, 0).unwrap();
                if deprecation_datetime < Utc::now().naive_utc() {
                    return true;
                }
            }
        }

        // Disabled, deleted, or renamed node with previously enforced contract.
        let breaking_change = if old.__base_attr__.enabled {
            format!(
                "Contracted model '{}' was deleted or renamed.",
                old.__common_attr__.unique_id
            )
        } else {
            format!(
                "Contracted model '{}' was disabled.",
                old.__common_attr__.unique_id
            )
        };

        self.log_contract_breaking_change_error(&breaking_change, &old.__common_attr__.name);

        // Otherwise, the contract has changed -- same_contract: False
        false
    }

    fn log_contract_breaking_change_error(&self, breaking_change: &String, node_name: &String) {
        let error_message = format!(
            "While comparing to previous project state, dbt detected a breaking change to an enforced contract.\n  - {breaking_change}\n\
            Consider making an additive (non-breaking) change instead, if possible.\n\
            Otherwise, create a new model version: https://docs.getdbt.com/docs/collaborate/govern/model-versions"
        );

        let breaking_change_message =
            format!("Breaking Change to Contract for model '{node_name}': {error_message}");
        emit_error_log_message(ErrorCode::Generic, breaking_change_message, None);
    }

    fn log_unversioned_breaking_change_warning(
        &self,
        breaking_change: &String,
        node_name: &String,
        file_path: String,
    ) {
        let warning_message = format!(
            "Breaking change to contracted, unversioned model {node_name} ({file_path})\
            \nWhile comparing to previous project state, dbt detected a breaking change to an unversioned model.\
            \n  - {breaking_change}\n"
        );

        emit_warn_log_message(ErrorCode::Generic, warning_message, None);
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct AdapterAttr {
    pub snowflake_attr: Option<Box<SnowflakeAttr>>,
    pub databricks_attr: Option<Box<DatabricksAttr>>,
    pub bigquery_attr: Option<Box<BigQueryAttr>>,
    pub redshift_attr: Option<Box<RedshiftAttr>>,
}

impl AdapterAttr {
    pub fn with_snowflake_attr(mut self, snowflake_attr: Option<Box<SnowflakeAttr>>) -> Self {
        self.snowflake_attr = snowflake_attr;
        self
    }

    pub fn with_databricks_attr(mut self, databricks_attr: Option<Box<DatabricksAttr>>) -> Self {
        self.databricks_attr = databricks_attr;
        self
    }

    pub fn with_bigquery_attr(mut self, bigquery_attr: Option<Box<BigQueryAttr>>) -> Self {
        self.bigquery_attr = bigquery_attr;
        self
    }

    pub fn with_redshift_attr(mut self, redshift_attr: Option<Box<RedshiftAttr>>) -> Self {
        self.redshift_attr = redshift_attr;
        self
    }

    /// Creates a new [AdapterAttr] from a given [&WarehouseSpecificNodeConfig] and adapter type string
    pub fn from_config_and_dialect(
        config: &WarehouseSpecificNodeConfig,
        adapter_type: AdapterType,
    ) -> Self {
        // TODO: Make all None inputs result in a None for that attr
        // TODO: Creation of configs should be delegated to the adapter specific attr struct
        // Here we naively initialize the resolved adapter attributes.
        // This allows propagation of configurations and switching on the adapter type
        // allows us to save memory for unused configurations.
        //
        // A further optimization can be done by only setting inner fields of [AdapterAttr] if at least one field of the
        // input is not None
        match adapter_type {
            AdapterType::Snowflake => {
                AdapterAttr::default().with_snowflake_attr(Some(Box::new(SnowflakeAttr {
                    table_tag: config.table_tag.clone(),
                    partition_by: config.partition_by.clone(),
                    row_access_policy: config.row_access_policy.clone(),
                    adapter_properties: config.adapter_properties.clone(),
                    external_volume: config.external_volume.clone(),
                    base_location_root: config.base_location_root.clone(),
                    base_location_subpath: config.base_location_subpath.clone(),
                    target_lag: config.target_lag.clone(),
                    snowflake_warehouse: config.snowflake_warehouse.clone(),
                    refresh_mode: config.refresh_mode.clone(),
                    initialize: config.initialize.clone(),
                    tmp_relation_type: config.tmp_relation_type.clone(),
                    query_tag: config.query_tag.clone(),
                    automatic_clustering: config.automatic_clustering,
                    copy_grants: config.copy_grants,
                    secure: config.secure,
                    transient: config.transient,
                })))
            }
            AdapterType::Postgres => AdapterAttr::default(),
            AdapterType::Bigquery => {
                AdapterAttr::default().with_bigquery_attr(Some(Box::new(BigQueryAttr {
                    description: config.description.clone(),
                    adapter_properties: config.adapter_properties.clone(),
                    external_volume: config.external_volume.clone(),
                    base_location_root: config.base_location_root.clone(),
                    base_location_subpath: config.base_location_subpath.clone(),
                    file_format: config.file_format.clone(),
                    partition_by: config.partition_by.clone(),
                    cluster_by: config.cluster_by.clone(),
                    hours_to_expiration: config.hours_to_expiration,
                    labels: config.labels.clone(),
                    labels_from_meta: config.labels_from_meta,
                    kms_key_name: config.kms_key_name.clone(),
                    resource_tags: config.resource_tags.clone(),
                    require_partition_filter: config.require_partition_filter,
                    partition_expiration_days: config.partition_expiration_days,
                    grant_access_to: config.grant_access_to.clone(),
                    partitions: config.partitions.clone(),
                    enable_refresh: config.enable_refresh,
                    refresh_interval_minutes: config.refresh_interval_minutes,
                    max_staleness: config.max_staleness.clone(),
                    jar_file_uri: config.jar_file_uri.clone(),
                    timeout: config.timeout,
                })))
            }
            AdapterType::Redshift => {
                AdapterAttr::default().with_redshift_attr(Some(Box::new(RedshiftAttr {
                    auto_refresh: config.auto_refresh,
                    backup: config.backup,
                    bind: config.bind,
                    dist: config.dist.clone(),
                    sort: config.sort.clone(),
                    sort_type: config.sort_type.clone(),
                })))
            }
            AdapterType::Databricks => {
                AdapterAttr::default().with_databricks_attr(Some(Box::new(DatabricksAttr {
                    adapter_properties: config.adapter_properties.clone(),
                    partition_by: config.partition_by.clone(),
                    file_format: config.file_format.clone(),
                    location_root: config.location_root.clone(),
                    tblproperties: config.tblproperties.clone(),
                    include_full_name_in_path: config.include_full_name_in_path,
                    liquid_clustered_by: config.liquid_clustered_by.clone(),
                    auto_liquid_cluster: config.auto_liquid_cluster,
                    clustered_by: config.clustered_by.clone(),
                    buckets: config.buckets,
                    catalog: config.catalog.clone(),
                    databricks_tags: config.databricks_tags.clone(),
                    compression: config.compression.clone(),
                    databricks_compute: config.databricks_compute.clone(),
                    target_alias: config.target_alias.clone(),
                    source_alias: config.source_alias.clone(),
                    matched_condition: config.matched_condition.clone(),
                    not_matched_condition: config.not_matched_condition.clone(),
                    not_matched_by_source_condition: config.not_matched_by_source_condition.clone(),
                    not_matched_by_source_action: config.not_matched_by_source_action.clone(),
                    merge_with_schema_evolution: config.merge_with_schema_evolution,
                    skip_matched_step: config.skip_matched_step,
                    skip_not_matched_step: config.skip_not_matched_step,
                    schedule: config.schedule.clone(),
                })))
            }
            _ => {
                // Unknown input, populate ALL adapter attributes to maximize compatibility downstream
                AdapterAttr::default()
                    .with_snowflake_attr(Some(Box::new(SnowflakeAttr {
                        table_tag: config.table_tag.clone(),
                        partition_by: config.partition_by.clone(),
                        row_access_policy: config.row_access_policy.clone(),
                        adapter_properties: config.adapter_properties.clone(),
                        external_volume: config.external_volume.clone(),
                        base_location_root: config.base_location_root.clone(),
                        base_location_subpath: config.base_location_subpath.clone(),
                        target_lag: config.target_lag.clone(),
                        snowflake_warehouse: config.snowflake_warehouse.clone(),
                        refresh_mode: config.refresh_mode.clone(),
                        initialize: config.initialize.clone(),
                        tmp_relation_type: config.tmp_relation_type.clone(),
                        query_tag: config.query_tag.clone(),
                        automatic_clustering: config.automatic_clustering,
                        copy_grants: config.copy_grants,
                        secure: config.secure,
                        transient: config.transient,
                    })))
                    .with_bigquery_attr(Some(Box::new(BigQueryAttr {
                        description: config.description.clone(),
                        adapter_properties: config.adapter_properties.clone(),
                        external_volume: config.external_volume.clone(),
                        base_location_root: config.base_location_root.clone(),
                        base_location_subpath: config.base_location_subpath.clone(),
                        file_format: config.file_format.clone(),
                        partition_by: config.partition_by.clone(),
                        cluster_by: config.cluster_by.clone(),
                        hours_to_expiration: config.hours_to_expiration,
                        labels: config.labels.clone(),
                        labels_from_meta: config.labels_from_meta,
                        kms_key_name: config.kms_key_name.clone(),
                        resource_tags: config.resource_tags.clone(),
                        require_partition_filter: config.require_partition_filter,
                        partition_expiration_days: config.partition_expiration_days,
                        grant_access_to: config.grant_access_to.clone(),
                        partitions: config.partitions.clone(),
                        enable_refresh: config.enable_refresh,
                        refresh_interval_minutes: config.refresh_interval_minutes,
                        max_staleness: config.max_staleness.clone(),
                        jar_file_uri: config.jar_file_uri.clone(),
                        timeout: config.timeout,
                    })))
                    .with_redshift_attr(Some(Box::new(RedshiftAttr {
                        auto_refresh: config.auto_refresh,
                        backup: config.backup,
                        bind: config.bind,
                        dist: config.dist.clone(),
                        sort: config.sort.clone(),
                        sort_type: config.sort_type.clone(),
                    })))
                    .with_databricks_attr(Some(Box::new(DatabricksAttr {
                        adapter_properties: config.adapter_properties.clone(),
                        partition_by: config.partition_by.clone(),
                        file_format: config.file_format.clone(),
                        location_root: config.location_root.clone(),
                        tblproperties: config.tblproperties.clone(),
                        include_full_name_in_path: config.include_full_name_in_path,
                        liquid_clustered_by: config.liquid_clustered_by.clone(),
                        auto_liquid_cluster: config.auto_liquid_cluster,
                        clustered_by: config.clustered_by.clone(),
                        buckets: config.buckets,
                        catalog: config.catalog.clone(),
                        databricks_tags: config.databricks_tags.clone(),
                        compression: config.compression.clone(),
                        databricks_compute: config.databricks_compute.clone(),
                        target_alias: config.target_alias.clone(),
                        source_alias: config.source_alias.clone(),
                        matched_condition: config.matched_condition.clone(),
                        not_matched_condition: config.not_matched_condition.clone(),
                        not_matched_by_source_condition: config
                            .not_matched_by_source_condition
                            .clone(),
                        not_matched_by_source_action: config.not_matched_by_source_action.clone(),
                        merge_with_schema_evolution: config.merge_with_schema_evolution,
                        skip_matched_step: config.skip_matched_step,
                        skip_not_matched_step: config.skip_not_matched_step,
                        schedule: config.schedule.clone(),
                    })))
            }
        }
    }
}

/// A resolved Snowflake configuration
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SnowflakeAttr {
    pub adapter_properties: Option<BTreeMap<String, YmlValue>>,
    pub partition_by: Option<PartitionConfig>,
    pub table_tag: Option<String>,
    pub row_access_policy: Option<String>,
    pub external_volume: Option<String>,
    pub base_location_root: Option<String>,
    pub base_location_subpath: Option<String>,
    pub target_lag: Option<String>,
    pub snowflake_warehouse: Option<String>,
    pub refresh_mode: Option<String>,
    pub initialize: Option<String>,
    pub tmp_relation_type: Option<String>,
    pub query_tag: Option<String>,
    pub automatic_clustering: Option<bool>,
    pub copy_grants: Option<bool>,
    pub secure: Option<bool>,
    pub transient: Option<bool>,
}

/// A resolved Databricks configuration
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DatabricksAttr {
    pub adapter_properties: Option<BTreeMap<String, YmlValue>>,
    pub partition_by: Option<PartitionConfig>,
    pub file_format: Option<String>,
    pub location_root: Option<String>,
    pub tblproperties: Option<BTreeMap<String, YmlValue>>,
    pub include_full_name_in_path: Option<bool>,
    pub liquid_clustered_by: Option<StringOrArrayOfStrings>,
    pub auto_liquid_cluster: Option<bool>,
    pub clustered_by: Option<String>,
    pub buckets: Option<i64>,
    pub catalog: Option<String>,
    pub databricks_tags: Option<BTreeMap<String, YmlValue>>,
    pub compression: Option<String>,
    pub databricks_compute: Option<String>,
    pub target_alias: Option<String>,
    pub source_alias: Option<String>,
    pub matched_condition: Option<String>,
    pub not_matched_condition: Option<String>,
    pub not_matched_by_source_condition: Option<String>,
    pub not_matched_by_source_action: Option<String>,
    pub merge_with_schema_evolution: Option<bool>,
    pub skip_matched_step: Option<bool>,
    pub skip_not_matched_step: Option<bool>,
    pub schedule: Option<ScheduleConfig>,
}

/// A resolved BigQuery configuration
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BigQueryAttr {
    pub description: Option<String>,
    pub adapter_properties: Option<BTreeMap<String, YmlValue>>,
    pub external_volume: Option<String>,
    pub file_format: Option<String>,
    pub base_location_root: Option<String>,
    pub base_location_subpath: Option<String>,
    pub partition_by: Option<PartitionConfig>,
    pub cluster_by: Option<BigqueryClusterConfig>,
    pub hours_to_expiration: Option<u64>,
    pub labels: Option<BTreeMap<String, String>>,
    pub labels_from_meta: Option<bool>,
    pub kms_key_name: Option<String>,
    pub resource_tags: Option<BTreeMap<String, String>>,
    pub require_partition_filter: Option<bool>,
    pub partition_expiration_days: Option<u64>,
    pub grant_access_to: Option<Vec<GrantAccessToTarget>>,
    pub partitions: Option<Vec<String>>,
    pub enable_refresh: Option<bool>,
    pub refresh_interval_minutes: Option<f64>,
    pub max_staleness: Option<String>,
    pub jar_file_uri: Option<String>,
    pub timeout: Option<u64>,
}

/// A resolved Redshift configuration
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RedshiftAttr {
    pub auto_refresh: Option<bool>,
    pub backup: Option<bool>,
    pub bind: Option<bool>,
    pub dist: Option<String>,
    pub sort: Option<StringOrArrayOfStrings>,
    pub sort_type: Option<String>,
}

fn default_false() -> bool {
    false
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DbtModelAttr {
    pub access: Access,
    pub group: Option<String>,
    #[serde(skip_serializing, default = "default_introspection")]
    pub introspection: IntrospectionKind,
    pub contract: Option<DbtContract>,
    pub incremental_strategy: Option<DbtIncrementalStrategy>,
    pub freshness: Option<ModelFreshness>,
    pub version: Option<StringOrInteger>,
    pub latest_version: Option<StringOrInteger>,
    pub constraints: Vec<ModelConstraint>,
    pub deprecation_date: Option<String>,
    // TODO: Investigate why primary_key is needed here (constraints already exist)
    pub primary_key: Vec<String>,
    pub time_spine: Option<TimeSpine>,
    pub event_time: Option<String>,
    // TODO(anna): See if we _need_ to put these here, or if they can somehow be added to AdapterAttr.
    pub catalog_name: Option<String>,
    pub table_format: Option<String>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtFunction {
    pub __common_attr__: CommonAttributes,

    pub __base_attr__: NodeBaseAttributes,

    pub __function_attr__: DbtFunctionAttr,

    // To be deprecated
    #[serde(rename = "config")]
    pub deprecated_config: FunctionConfig,

    pub __other__: BTreeMap<String, YmlValue>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtFunctionAttr {
    pub access: Access,
    pub group: Option<String>,
    pub language: Option<String>,
    pub on_configuration_change: Option<String>,
    pub returns: Option<FunctionReturnType>,
    pub arguments: Option<Vec<FunctionArgument>>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TimeSpine {
    pub node_relation: NodeRelation,
    pub primary_column: TimeSpinePrimaryColumn,
    pub custom_granularities:
        Vec<crate::schemas::properties::model_properties::TimeSpineCustomGranularity>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TimeSpinePrimaryColumn {
    pub name: String,
    pub time_granularity: crate::schemas::dbt_column::Granularity,
}

fn default_introspection() -> IntrospectionKind {
    IntrospectionKind::None
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::{ModelConfig, hooks_equal, persist_docs_configs_equal, quoting_equal};
    use crate::schemas::common::{Hooks, PersistDocsConfig};
    use dbt_serde_yaml::Verbatim;

    type YmlValue = dbt_serde_yaml::Value;

    #[test]
    fn test_hooks_equal_none_vs_empty_array() {
        // Test that None is equal to Some(ArrayOfStrings([]))
        let none_hooks: Verbatim<Option<Hooks>> = Verbatim::from(None);
        let empty_array_hooks: Verbatim<Option<Hooks>> =
            Verbatim::from(Some(Hooks::ArrayOfStrings(vec![])));

        // Test both directions
        assert!(hooks_equal(&none_hooks, &empty_array_hooks));
        assert!(hooks_equal(&empty_array_hooks, &none_hooks));
    }

    #[test]
    fn test_persist_docs_configs_equal_none_vs_empty() {
        // Test that None is equal to Some(PersistDocsConfig { columns: None, relation: None })
        let none_config: Option<PersistDocsConfig> = None;
        let empty_config: Option<PersistDocsConfig> = Some(PersistDocsConfig {
            columns: None,
            relation: None,
        });

        // Test both directions
        assert!(persist_docs_configs_equal(&none_config, &empty_config));
        assert!(persist_docs_configs_equal(&empty_config, &none_config));

        // Also test that two None values are equal
        assert!(persist_docs_configs_equal(&none_config, &none_config));

        // And that two empty configs are equal
        assert!(persist_docs_configs_equal(&empty_config, &empty_config));
    }

    #[test]
    fn test_quoting_equal_various_cases() {
        use crate::schemas::common::DbtQuoting;

        // Case 1: None vs Some with all fields None
        let none_quoting: Option<DbtQuoting> = None;
        let all_none_quoting: Option<DbtQuoting> = Some(DbtQuoting {
            database: None,
            identifier: None,
            schema: None,
            snowflake_ignore_case: None,
        });

        assert!(quoting_equal(&none_quoting, &all_none_quoting));
        assert!(quoting_equal(&all_none_quoting, &none_quoting));

        // Case 2: None vs Some with all fields Some(false)
        let all_false_quoting: Option<DbtQuoting> = Some(DbtQuoting {
            database: Some(false),
            identifier: Some(false),
            schema: Some(false),
            snowflake_ignore_case: Some(false),
        });

        assert!(quoting_equal(&none_quoting, &all_false_quoting));
        assert!(quoting_equal(&all_false_quoting, &none_quoting));

        // Case 3: Some with all None vs Some with all Some(false)
        assert!(quoting_equal(&all_none_quoting, &all_false_quoting));
        assert!(quoting_equal(&all_false_quoting, &all_none_quoting));

        // Case 4: Mixed None and Some(false) should be equal
        let mixed_quoting_1: Option<DbtQuoting> = Some(DbtQuoting {
            database: Some(false),
            identifier: None,
            schema: Some(false),
            snowflake_ignore_case: None,
        });

        let mixed_quoting_2: Option<DbtQuoting> = Some(DbtQuoting {
            database: None,
            identifier: Some(false),
            schema: None,
            snowflake_ignore_case: Some(false),
        });

        assert!(quoting_equal(&mixed_quoting_1, &mixed_quoting_2));

        // Case 5: Some(true) should NOT be equal to None or Some(false)
        let some_true_quoting: Option<DbtQuoting> = Some(DbtQuoting {
            database: Some(true),
            identifier: Some(false),
            schema: None,
            snowflake_ignore_case: Some(false),
        });

        assert!(!quoting_equal(&some_true_quoting, &none_quoting));
        assert!(!quoting_equal(&some_true_quoting, &all_none_quoting));
        assert!(!quoting_equal(&some_true_quoting, &all_false_quoting));

        // Case 6: Two identical configs with Some(true) should be equal
        let another_true_quoting: Option<DbtQuoting> = Some(DbtQuoting {
            database: Some(true),
            identifier: Some(false),
            schema: None,
            snowflake_ignore_case: Some(false),
        });

        assert!(quoting_equal(&some_true_quoting, &another_true_quoting));
    }

    #[test]
    fn test_deserialize_wo_meta() {
        let config: YmlValue = dbt_serde_yaml::from_str(
            r#"
            enabled: true
            "#,
        )
        .expect("Failed to deserialize model config");

        let config = ModelConfig::deserialize(config);
        if let Err(err) = config {
            panic!("Could not deserialize and failed with the following error: {err}");
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DbtAnalysis {
    pub __common_attr__: CommonAttributes,

    pub __base_attr__: NodeBaseAttributes,

    pub __analysis_attr__: DbtAnalysisAttr,

    /// To be deprecated
    #[serde(rename = "config")]
    pub deprecated_config: super::project::configs::analysis_config::AnalysesConfig,

    pub __other__: BTreeMap<String, YmlValue>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtAnalysisAttr {
    // Analysis nodes have minimal attributes since they're for ad-hoc querying
    // Most functionality comes from the base attributes and config
}

impl InternalDbtNode for DbtAnalysis {
    fn common(&self) -> &CommonAttributes {
        &self.__common_attr__
    }

    fn base(&self) -> &NodeBaseAttributes {
        &self.__base_attr__
    }

    fn base_mut(&mut self) -> &mut NodeBaseAttributes {
        &mut self.__base_attr__
    }

    fn common_mut(&mut self) -> &mut CommonAttributes {
        &mut self.__common_attr__
    }

    fn resource_type(&self) -> NodeType {
        NodeType::Analysis
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn serialize_inner(
        &self,
        mode: crate::schemas::serialization_utils::SerializationMode,
    ) -> YmlValue {
        crate::schemas::serialization_utils::serialize_with_mode(self, mode)
    }

    fn has_same_config(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_analysis) = other.as_any().downcast_ref::<DbtAnalysis>() {
            let self_config = &self.deprecated_config;
            let other_config = &other_analysis.deprecated_config;

            self_config.enabled == other_config.enabled
                && self_config.static_analysis == other_config.static_analysis
        } else {
            false
        }
    }

    fn has_same_content(&self, other: &dyn InternalDbtNode) -> bool {
        if let Some(other_analysis) = other.as_any().downcast_ref::<DbtAnalysis>() {
            self.__common_attr__.checksum == other_analysis.__common_attr__.checksum
        } else {
            false
        }
    }

    fn set_detected_introspection(&mut self, _introspection: IntrospectionKind) {
        panic!("DbtAnalysis does not support setting detected_unsafe");
    }
}

impl InternalDbtNodeAttributes for DbtAnalysis {
    fn static_analysis(&self) -> Spanned<StaticAnalysisKind> {
        self.__base_attr__.static_analysis.clone()
    }

    fn set_quoting(&mut self, quoting: ResolvedQuoting) {
        self.__base_attr__.quoting = quoting;
    }

    fn set_static_analysis(&mut self, static_analysis: Spanned<StaticAnalysisKind>) {
        self.__base_attr__.static_analysis = static_analysis;
    }

    fn get_access(&self) -> Option<Access> {
        None // Analysis nodes don't have access controls
    }

    fn get_group(&self) -> Option<String> {
        None // Analysis nodes don't have groups
    }

    fn search_name(&self) -> String {
        self.__common_attr__.name.clone()
    }

    fn selector_string(&self) -> String {
        format!("analysis:{}", self.__common_attr__.name)
    }

    fn serialized_config(&self) -> YmlValue {
        dbt_serde_yaml::to_value(&self.deprecated_config).expect("Failed to serialize to YAML")
    }
}
pub fn is_invalid_for_relation_comparison(node: &dyn InternalDbtNode) -> bool {
    node.resource_type() == NodeType::UnitTest
}
