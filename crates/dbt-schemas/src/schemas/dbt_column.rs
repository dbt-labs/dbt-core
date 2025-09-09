use std::{collections::BTreeMap, sync::Arc};

use dbt_common::FsResult;
use dbt_serde_yaml::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;

use crate::schemas::{
    semantic_layer::semantic_manifest::SemanticLayerElementConfig, serde::StringOrArrayOfStrings,
};

use super::{common::Constraint, data_tests::DataTests};

#[skip_serializing_none]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtColumn {
    pub name: String,
    pub data_type: Option<String>,
    pub description: Option<String>,
    pub constraints: Vec<Constraint>,
    pub meta: BTreeMap<String, YmlValue>,
    pub tags: Vec<String>,
    pub policy_tags: Option<Vec<String>>,
    pub quote: Option<bool>,
    #[serde(default, rename = "config")]
    pub deprecated_config: ColumnConfig,
}

pub type DbtColumnRef = Arc<DbtColumn>;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ColumnProperties {
    pub name: String,
    pub data_type: Option<String>,
    pub description: Option<String>,
    pub constraints: Option<Vec<Constraint>>,
    pub tests: Option<Vec<DataTests>>,
    pub data_tests: Option<Vec<DataTests>>,
    pub granularity: Option<ColumnPropertiesGranularity>,
    pub policy_tags: Option<Vec<String>>,
    pub quote: Option<bool>,
    pub config: Option<ColumnConfig>,

    pub entity: Option<Entity>,
    pub dimension: Option<ColumnPropertiesDimension>,
}

#[derive(Deserialize, Serialize, Debug, Clone, Default, JsonSchema, Eq, PartialEq)]
#[allow(non_camel_case_types)]
pub enum ColumnPropertiesGranularity {
    #[default]
    nanosecond,
    microsecond,
    millisecond,
    second,
    minute,
    hour,
    day,
    week,
    month,
    quarter,
    year,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, Default, PartialEq, Eq)]
pub struct ColumnConfig {
    #[serde(default)]
    pub tags: Option<StringOrArrayOfStrings>,
    pub meta: Option<BTreeMap<String, YmlValue>>,
}

/// Represents column inheritance rules for a model version
#[derive(Debug, Clone)]
pub struct ColumnInheritanceRules {
    includes: Vec<String>, // Empty vec means include all
    excludes: Vec<String>,
}

impl ColumnInheritanceRules {
    // Given a column block in a versioned model, return the includes and excludes for that model
    pub fn from_version_columns(columns: &dbt_serde_yaml::Value) -> Option<Self> {
        if let dbt_serde_yaml::Value::Sequence(cols, _) = columns {
            for col in cols {
                if let dbt_serde_yaml::Value::Mapping(map, _) = col {
                    // Only create inheritance rules if there's an include or exclude
                    let include_key = dbt_serde_yaml::Value::string("include".to_string());
                    let exclude_key = dbt_serde_yaml::Value::string("exclude".to_string());

                    if map.contains_key(&include_key) || map.contains_key(&exclude_key) {
                        let includes = map
                            .get(&include_key)
                            .map(|v| match v {
                                dbt_serde_yaml::Value::String(s, _) if s == "*" || s == "all" => {
                                    Vec::new()
                                } // Empty vec means include all
                                dbt_serde_yaml::Value::Sequence(arr, _) => arr
                                    .iter()
                                    .filter_map(|v| match v {
                                        dbt_serde_yaml::Value::String(s, _) => Some(s.clone()),
                                        _ => None,
                                    })
                                    .collect(),
                                dbt_serde_yaml::Value::String(s, _) => vec![s.clone()],
                                _ => Vec::new(),
                            })
                            .unwrap_or_default(); // Default to empty vec (include all)

                        let excludes = map
                            .get(&exclude_key)
                            .map(|v| match v {
                                dbt_serde_yaml::Value::Sequence(arr, _) => arr
                                    .iter()
                                    .filter_map(|v| match v {
                                        dbt_serde_yaml::Value::String(s, _) => Some(s.clone()),
                                        _ => None,
                                    })
                                    .collect(),
                                dbt_serde_yaml::Value::String(s, _) => vec![s.clone()],
                                _ => Vec::new(),
                            })
                            .unwrap_or_default();

                        return Some(ColumnInheritanceRules { includes, excludes });
                    }
                }
            }
        }
        None // No inheritance rules specified means use default (inherit all)
    }

    /// given a column name, return true if it should be included in the tests based on the includes and excludes and inheritance rules
    pub fn should_include_column(&self, column_name: &str) -> bool {
        if self.includes.is_empty() {
            // Empty includes means include all except excluded
            !self.excludes.contains(&column_name.to_string())
        } else {
            // Specific includes: must be in includes and not in excludes
            self.includes.contains(&column_name.to_string())
                && !self.excludes.contains(&column_name.to_string())
        }
    }
}

/// Process columns by merging parent config with each column's config.
/// Returns a BTreeMap of column name to DbtColumn.
pub fn process_columns(
    columns: Option<&Vec<ColumnProperties>>,
    meta: Option<BTreeMap<String, YmlValue>>,
    tags: Option<Vec<String>>,
) -> FsResult<BTreeMap<String, DbtColumnRef>> {
    Ok(columns
        .map(|cols| {
            cols.iter()
                .map(|cp| {
                    let (cp_meta, cp_tags) = cp
                        .config
                        .clone()
                        .map(|c| (c.meta, c.tags))
                        .unwrap_or_default();

                    Ok(Arc::new(DbtColumn {
                        name: cp.name.clone(),
                        data_type: cp.data_type.clone(),
                        description: cp.description.clone(),
                        constraints: cp.constraints.clone().unwrap_or_default(),
                        meta: cp_meta.unwrap_or(meta.clone().unwrap_or_default()),
                        tags: cp_tags
                            .map(|t| t.into())
                            .unwrap_or(tags.clone().unwrap_or_default()),
                        policy_tags: cp.policy_tags.clone(),
                        quote: cp.quote,
                        deprecated_config: cp.config.clone().unwrap_or_default(),
                    }))
                })
                .collect::<Result<Vec<DbtColumnRef>, Box<dyn std::error::Error>>>()
        })
        .transpose()?
        .map(|cols| {
            cols.into_iter()
                .map(|c| (c.name.clone(), c))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default())
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, Eq, PartialEq)]
#[serde(untagged)]
pub enum ColumnPropertiesDimension {
    DimensionConfig(ColumnPropertiesDimensionConfig),
    DimensionType(ColumnPropertiesDimensionType),
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, Eq, PartialEq)]
#[allow(non_camel_case_types)]
pub enum ColumnPropertiesDimensionType {
    categorical,
    time,
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, Eq, PartialEq)]
pub struct ColumnPropertiesDimensionConfig {
    #[serde(rename = "type")]
    pub type_: ColumnPropertiesDimensionType,
    pub is_partition: Option<bool>,
    pub label: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub config: Option<SemanticLayerElementConfig>,
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
#[serde(untagged)]
pub enum Entity {
    EntityConfig(EntityConfig),
    EntityType(ColumnPropertiesEntityType),
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, Eq, PartialEq)]
#[allow(non_camel_case_types)]
pub enum ColumnPropertiesEntityType {
    foreign,
    natural,
    primary,
    unique,
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct EntityConfig {
    #[serde(rename = "type")]
    pub type_: ColumnPropertiesEntityType,
    pub name: Option<String>,
    pub description: Option<String>,
    pub label: Option<String>,
    pub config: Option<SemanticLayerElementConfig>,
}
