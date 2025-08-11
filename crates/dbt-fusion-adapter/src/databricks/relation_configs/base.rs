use crate::databricks::relation_configs::DatabricksRelationConfig;
use crate::databricks::relation_configs::column_comments::ColumnCommentsConfig;
use crate::databricks::relation_configs::comment::CommentConfig;
use crate::databricks::relation_configs::constraints::ConstraintsConfig;
use crate::databricks::relation_configs::liquid_clustering::LiquidClusteringConfig;
use crate::databricks::relation_configs::partitioning::PartitionedByConfig;
use crate::databricks::relation_configs::query::QueryConfig;
use crate::databricks::relation_configs::refresh::RefreshConfig;
use crate::databricks::relation_configs::tags::TagsConfig;
use crate::databricks::relation_configs::tblproperties::TblPropertiesConfig;

use crate::AdapterResult;
use crate::funcs::none_value;
use dbt_agate::AgateTable;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use dbt_schemas::schemas::relations::relation_configs::{
    BaseRelationChangeSet, BaseRelationConfig, ComponentConfig, RelationChangeSet,
};
use minijinja::arg_utils::ArgParser;
use minijinja::listener::RenderingEventListener;
use minijinja::value::{Enumerator, Object};
use minijinja::{Error as MiniJinjaError, ErrorKind, State, Value as MiniJinjaValue};
use serde::{Deserialize, Serialize};
type YmlValue = dbt_serde_yaml::Value;

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum DatabricksRelationMetadataKey {
    InfoSchemaViews,
    InfoSchemaTags,
    DescribeExtended,
    ShowTblProperties,
    ColumnMasks,
    PrimaryKeyConstraints,
    ForeignKeyConstraints,
    NonNullConstraints,
}

// string conversions based on string keys from:
// https://github.com/databricks/dbt-databricks/blob/9e2566fdb56318cb7a59a4492f96c7aaa7af73b0/dbt/adapters/databricks/impl.py#L914-L1021
impl From<DatabricksRelationMetadataKey> for String {
    fn from(key: DatabricksRelationMetadataKey) -> Self {
        match key {
            DatabricksRelationMetadataKey::InfoSchemaViews => {
                "information_schema.views".to_string()
            }
            DatabricksRelationMetadataKey::InfoSchemaTags => "information_schema.tags".to_string(),
            DatabricksRelationMetadataKey::DescribeExtended => "describe_extended".to_string(),
            DatabricksRelationMetadataKey::ShowTblProperties => "show_tblproperties".to_string(),
            DatabricksRelationMetadataKey::ColumnMasks => "column_masks".to_string(),
            DatabricksRelationMetadataKey::PrimaryKeyConstraints => {
                "primary_key_constraints".to_string()
            }
            DatabricksRelationMetadataKey::ForeignKeyConstraints => {
                "foreign_key_constraints".to_string()
            }
            DatabricksRelationMetadataKey::NonNullConstraints => {
                "non_null_constraint_columns".to_string()
            }
        }
    }
}

impl From<&DatabricksRelationMetadataKey> for String {
    fn from(key: &DatabricksRelationMetadataKey) -> Self {
        key.clone().into()
    }
}

// TODO: Create types for each of these and use an enum
#[derive(Debug, Clone, Default)]
pub struct DatabricksRelationResults(BTreeMap<DatabricksRelationMetadataKey, AgateTable>);

impl DerefMut for DatabricksRelationResults {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Deref for DatabricksRelationResults {
    type Target = BTreeMap<DatabricksRelationMetadataKey, AgateTable>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Default)]
pub struct DatabricksRelationResultsBuilder {
    results: DatabricksRelationResults,
}

macro_rules! result_builder_entries {
    ($($method_name:ident => $key:expr),* $(,)?) => {
        $(
            pub fn $method_name(mut self, table: AgateTable) -> Self {
                self.results.insert($key, table);
                self
            }
        )*
    };
}

impl DatabricksRelationResultsBuilder {
    pub fn new() -> Self {
        Self {
            results: DatabricksRelationResults::default(),
        }
    }
    result_builder_entries! {
        with_info_schema_views => DatabricksRelationMetadataKey::InfoSchemaViews,
        with_info_schema_tags => DatabricksRelationMetadataKey::InfoSchemaTags,
        with_describe_extended => DatabricksRelationMetadataKey::DescribeExtended,
        with_show_tblproperties => DatabricksRelationMetadataKey::ShowTblProperties,
        with_column_masks => DatabricksRelationMetadataKey::ColumnMasks,
        with_primary_key_constraints => DatabricksRelationMetadataKey::PrimaryKeyConstraints,
        with_foreign_key_constraints => DatabricksRelationMetadataKey::ForeignKeyConstraints,
        with_non_null_constraints => DatabricksRelationMetadataKey::NonNullConstraints,
    }

    pub fn build(self) -> DatabricksRelationResults {
        self.results
    }
}

/// Trait for encapsulating a single component of a Databricks relation config.
///
/// Ex: A materialized view has a `query` component, which is a string that if changed, requires a
/// full refresh.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DatabricksComponentConfig {
    ColumnComments(ColumnCommentsConfig),
    Comment(CommentConfig),
    Constraints(ConstraintsConfig),
    LiquidClustering(LiquidClusteringConfig),
    PartitionedBy(PartitionedByConfig),
    Query(QueryConfig),
    Refresh(RefreshConfig),
    Tags(TagsConfig),
    TblProperties(TblPropertiesConfig),
}

/// Get the config that must be applied when this component differs from the existing
/// version. This method is intended to only be called on the new version (i.e. the version
/// specified in the dbt project).
///
/// If the difference does not require any changes to the existing relation, this method should
/// return None. If some partial change can be applied to the existing relation, the
/// implementing component should override this method to return an instance representing the
/// partial change; however, care should be taken to ensure that the returned object retains
/// the complete config specified in the dbt project, so as to support rendering the `create`
/// as well as the `alter` statements, for the case where a different component requires full
/// refresh.
impl ComponentConfig for DatabricksComponentConfig {
    fn get_diff(&self, other: &dyn ComponentConfig) -> Option<Arc<dyn ComponentConfig>> {
        // only compatible with itself
        let other = other.as_any().downcast_ref::<DatabricksComponentConfig>()?;
        match (self, other) {
            (DatabricksComponentConfig::Query(a), DatabricksComponentConfig::Query(b)) => {
                a.get_diff(b).map(|diff| {
                    Arc::new(DatabricksComponentConfig::Query(diff)) as Arc<dyn ComponentConfig>
                })
            }
            (
                DatabricksComponentConfig::ColumnComments(a),
                DatabricksComponentConfig::ColumnComments(b),
            ) => a.get_diff(b).map(|diff| {
                Arc::new(DatabricksComponentConfig::ColumnComments(diff))
                    as Arc<dyn ComponentConfig>
            }),
            _ => {
                if self != other {
                    Some(Arc::new(self.clone()) as Arc<dyn ComponentConfig>)
                } else {
                    None
                }
            }
        }
    }

    fn as_value(&self) -> MiniJinjaValue {
        MiniJinjaValue::from_serialize(self)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Structure representing a set of changes to be applied to a relation
pub type DatabricksRelationChangeSet = BaseRelationChangeSet;

#[derive(Debug, Clone)]
pub struct DatabricksRelationChangeSetObject(pub BaseRelationChangeSet);

impl Object for DatabricksRelationChangeSetObject {
    fn get_value(self: &Arc<Self>, key: &MiniJinjaValue) -> Option<MiniJinjaValue> {
        match key.as_str()? {
            "changes" => Some(MiniJinjaValue::from_object(RelationChangeSetChangesObject(
                Arc::new(self.0.changes().clone()),
            ))),
            "requires_full_refresh" => Some(self.0.requires_full_refresh().into()),
            _ => None,
        }
    }
    fn enumerate(self: &Arc<Self>) -> Enumerator {
        Enumerator::Str(&["changes", "requires_full_refresh"])
    }
}

#[derive(Debug, Clone)]
pub struct RelationChangeSetChangesObject(Arc<BTreeMap<String, Arc<dyn ComponentConfig>>>);

impl Object for RelationChangeSetChangesObject {
    fn call_method(
        self: &Arc<Self>,
        _state: &State,
        name: &str,
        args: &[MiniJinjaValue],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<MiniJinjaValue, MiniJinjaError> {
        let mut parser = ArgParser::new(args, None);
        match name {
            // support example `_configuration_changes.changes.get("tags", None)`
            "get" => {
                let key = parser.get::<MiniJinjaValue>("key")?;
                let key = key.as_str().ok_or_else(|| {
                    MiniJinjaError::new(ErrorKind::InvalidArgument, "key must be a string")
                })?;
                let value = self.0.get(key);

                if let Some(c) = value {
                    Ok(c.as_value())
                } else {
                    Ok(none_value())
                }
            }
            _ => Err(MiniJinjaError::new(
                ErrorKind::UnknownMethod,
                format!("RelationChangeSetChangesObject has no method named '{name}'"),
            )),
        }
    }

    fn get_value(self: &Arc<Self>, key: &MiniJinjaValue) -> Option<MiniJinjaValue> {
        let key = key.as_str()?;
        let value = self.0.get(key)?;
        Some(value.as_value())
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        Enumerator::Iter(Box::new(
            self.0
                .keys()
                .map(MiniJinjaValue::from)
                .collect::<Vec<_>>()
                .into_iter(),
        ))
    }
}

pub trait DatabricksComponentProcessorProperties {
    fn name(&self) -> &'static str;
}

/// Trait for processors that can extract components from relation results or configs
pub trait DatabricksComponentProcessor:
    Send + Sync + DatabricksComponentProcessorProperties + Debug
{
    /// Extract the component from the results of a query against the existing relation.
    #[allow(clippy::wrong_self_convention)]
    fn from_relation_results(
        &self,
        _row: &DatabricksRelationResults,
    ) -> Option<DatabricksComponentConfig>;

    /// Extract the component from the node.
    ///
    /// While some components, e.g. query, can be extracted directly from the model node,
    /// specialized Databricks config can be found in model_node.config.extra.
    #[allow(clippy::wrong_self_convention)]
    fn from_relation_config(
        &self,
        model_node: &dyn InternalDbtNodeAttributes,
    ) -> AdapterResult<Option<DatabricksComponentConfig>>;
}

#[derive(Debug, Clone)]
pub struct DatabricksRelationConfigBaseObject(Arc<dyn DatabricksRelationConfigBase>);

impl Deref for DatabricksRelationConfigBaseObject {
    type Target = Arc<dyn DatabricksRelationConfigBase>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DatabricksRelationConfigBaseObject {
    pub fn new(inner: Arc<dyn DatabricksRelationConfigBase>) -> Self {
        Self(inner)
    }
}

impl Object for DatabricksRelationConfigBaseObject {
    fn call_method(
        self: &Arc<Self>,
        _state: &State,
        name: &str,
        args: &[MiniJinjaValue],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<MiniJinjaValue, MiniJinjaError> {
        let mut parser = ArgParser::new(args, None);
        match name {
            "get_changeset" => {
                let existing = parser.get::<MiniJinjaValue>("existing_relation")?;
                if let Some(changeset) = self.get_changeset_for_object(existing) {
                    let changes = changeset.changes().clone();
                    let requires_full_refresh = changeset.requires_full_refresh();
                    Ok(MiniJinjaValue::from_object(
                        DatabricksRelationChangeSetObject(BaseRelationChangeSet::new(
                            changes,
                            requires_full_refresh,
                        )),
                    ))
                } else {
                    Ok(none_value())
                }
            }
            _ => unimplemented!("RelationConfigBaseObject does not support method: {}", name),
        }
    }

    fn get_value(self: &Arc<Self>, key: &MiniJinjaValue) -> Option<MiniJinjaValue> {
        let key = key.as_str()?;
        self.config().get(key).map(MiniJinjaValue::from_serialize)
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        Enumerator::Iter(Box::new(
            self.config()
                .keys()
                .map(MiniJinjaValue::from)
                .collect::<Vec<_>>()
                .into_iter(),
        ))
    }
}

/// Base trait for relation configurations - simplified version
pub trait DatabricksRelationConfigBase: BaseRelationConfig + Send + Sync + Debug {
    fn config_components_(&self) -> Vec<Arc<dyn DatabricksComponentProcessor>>;

    fn config(&self) -> BTreeMap<String, DatabricksComponentConfig>;

    fn get_component(&self, key: &str) -> Option<DatabricksComponentConfig>;

    fn get_changeset_default(
        &self,
        existing: MiniJinjaValue,
    ) -> Option<Arc<dyn RelationChangeSet>> {
        let mut changes = BTreeMap::new();
        let existing = existing.downcast_object::<DatabricksRelationConfigBaseObject>()?;

        for component in self.config_components_() {
            let key = component.name();
            if let (Some(value), Some(existing_value)) =
                (self.get_component(key), existing.get_component(key))
            {
                if let Some(diff) = value.get_diff(&existing_value) {
                    changes.insert(key.to_string(), diff);
                }
            }
        }

        if !changes.is_empty() {
            Some(Arc::new(DatabricksRelationChangeSet::new(changes, false)))
        } else {
            None
        }
    }

    /// Get the changeset that must be applied to the existing relation to make it match the
    /// current state of the dbt project. If no changes are required, this method should return
    /// None.
    fn get_changeset(&self, existing: MiniJinjaValue) -> Option<Arc<dyn RelationChangeSet>> {
        self.get_changeset_default(existing)
    }

    /// Get the changeset for object method calls (to avoid name conflicts)
    fn get_changeset_for_object(
        &self,
        existing: MiniJinjaValue,
    ) -> Option<Arc<dyn RelationChangeSet>> {
        DatabricksRelationConfigBase::get_changeset(self, existing)
    }
}

/// Get a value from the config.extra dictionary, or None if it is not present.
/// reference: https://github.com/databricks/dbt-databricks/blob/6cd74dd5a9f46a9f29e49d1b077b63b73646507a/dbt/adapters/databricks/relation_configs/base.py#L143
pub fn get_config_value(config: &dyn InternalDbtNodeAttributes, key: &str) -> Option<YmlValue> {
    config.meta().get(key).cloned()
}

/// Build the relation config from the results of a query against the existing relation.
pub fn from_results<T: DatabricksRelationConfig>(
    results: DatabricksRelationResults,
) -> AdapterResult<T> {
    let mut config_dict = BTreeMap::new();
    for component in T::config_components() {
        if let Some(relation_component) = component.from_relation_results(&results) {
            config_dict.insert(component.name().to_string(), relation_component);
        }
    }
    Ok(T::new(config_dict))
}

/// Build the relation config from a model node.
pub fn from_relation_config<T: DatabricksRelationConfig>(
    relation_config: &dyn InternalDbtNodeAttributes,
) -> AdapterResult<BTreeMap<String, DatabricksComponentConfig>> {
    let mut config_dict = BTreeMap::new();
    for component in T::config_components() {
        if let Some(relation_component) = component.from_relation_config(relation_config)? {
            config_dict.insert(component.name().to_string(), relation_component);
        }
    }
    Ok(config_dict)
}
