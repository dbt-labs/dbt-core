use indexmap::IndexMap;
use std::{collections::BTreeMap, sync::Arc};

use dbt_common::FsResult;
use dbt_yaml::{DbtSchema, UntaggedEnumDeserialize};
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::skip_serializing_none;
use strum::Display;

// Type aliases for clarity
type YmlValue = dbt_yaml::Value;

use crate::schemas::{
    common::DimensionValidityParams, semantic_layer::semantic_manifest::SemanticLayerElementConfig,
    serde::StringOrArrayOfStrings,
};

use super::{common::Constraint, data_tests::DataTests};

/// The BaseColumn as implemented by dbt Core.
///
/// This is used to deserialize columns from Jinja that produces them, for example
/// the public API macros for `get_columns_in_relation()`
#[derive(Deserialize, Debug)]
pub struct DbtCoreBaseColumn {
    pub name: String,
    pub dtype: String,
    pub char_size: Option<u32>,
    pub numeric_precision: Option<u64>,
    pub numeric_scale: Option<u64>,
}

#[skip_serializing_none]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Default, Clone)]
#[serde(rename_all = "snake_case")]
pub struct DbtColumn {
    pub name: String,
    pub data_type: Option<String>,
    #[serialize_always]
    #[serde(serialize_with = "serialize_dbt_column_desc")]
    pub description: Option<String>,
    pub constraints: Vec<Constraint>,
    pub meta: IndexMap<String, YmlValue>,
    pub tags: Vec<String>,
    pub policy_tags: Option<Vec<String>>,
    pub databricks_tags: Option<BTreeMap<String, YmlValue>>,
    pub column_mask: Option<ColumnMask>,
    pub quote: Option<bool>,
    #[serde(default, rename = "config")]
    pub deprecated_config: ColumnConfig,
}

fn serialize_dbt_column_desc<S>(description: &Option<String>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(description.as_deref().unwrap_or(""))
}

pub type DbtColumnRef = Arc<DbtColumn>;

/// Serialize and deserialize as a map to maintain Jinja behavior
pub fn serialize_dbt_columns<S>(columns: &Vec<DbtColumnRef>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut map = s.serialize_map(Some(columns.len()))?;
    for col in columns {
        map.serialize_entry(&col.name.clone(), col)?;
    }
    map.end()
}

pub fn deserialize_dbt_columns<'de, D>(deserializer: D) -> Result<Vec<DbtColumnRef>, D::Error>
where
    D: Deserializer<'de>,
{
    struct DbtColumnVisitor;

    impl<'de> Visitor<'de> for DbtColumnVisitor {
        type Value = Vec<DbtColumnRef>;

        fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
        where
            M: MapAccess<'de>,
        {
            let mut columns = Vec::new();
            while let Some((_key, value)) =
                map.next_entry::<serde::de::IgnoredAny, DbtColumnRef>()?
            {
                columns.push(value)
            }
            Ok(columns)
        }

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "a map of column names to columns")
        }
    }

    deserializer.deserialize_map(DbtColumnVisitor)
}

#[skip_serializing_none]
#[derive(Default, Deserialize, Serialize, Debug, Clone, DbtSchema)]
pub struct ColumnProperties {
    pub name: String,
    pub data_type: Option<String>,
    pub description: Option<String>,
    pub constraints: Option<Vec<Constraint>>,
    pub tests: Option<Vec<DataTests>>,
    pub data_tests: Option<Vec<DataTests>>,
    pub granularity: Option<Granularity>,
    pub policy_tags: Option<Vec<String>>,
    pub databricks_tags: Option<BTreeMap<String, YmlValue>>,
    pub column_mask: Option<ColumnMask>,
    pub quote: Option<bool>,
    pub config: Option<ColumnConfig>,

    pub entity: Option<Entity>,
    pub dimension: Option<ColumnPropertiesDimension>,
}

#[derive(Debug, Default, Deserialize, Serialize, Clone, DbtSchema, Eq, PartialEq)]
pub struct ColumnMask {
    pub function: String,
    pub using_columns: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone, Default, DbtSchema, Eq, PartialEq, Display)]
#[allow(non_camel_case_types)]
pub enum Granularity {
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
#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema, Default, PartialEq, Eq)]
pub struct ColumnConfig {
    #[serde(default)]
    pub tags: Option<StringOrArrayOfStrings>,
    pub meta: Option<IndexMap<String, YmlValue>>,
    pub databricks_tags: Option<BTreeMap<String, YmlValue>>,
    pub policy_tags: Option<Vec<String>>,
}

/// Represents column inheritance rules for a model version
#[derive(Debug, Clone)]
pub struct ColumnInheritanceRules {
    includes: Vec<String>, // Empty vec means include all
    excludes: Vec<String>,
}

impl ColumnInheritanceRules {
    // Given a column block in a versioned model, return the includes and excludes for that model
    pub fn from_version_columns(columns: &dbt_yaml::Value) -> Option<Self> {
        if let dbt_yaml::Value::Sequence(cols, _) = columns {
            for col in cols {
                if let dbt_yaml::Value::Mapping(map, _) = col {
                    // Only create inheritance rules if there's an include or exclude
                    let include_key = dbt_yaml::Value::string("include".to_string());
                    let exclude_key = dbt_yaml::Value::string("exclude".to_string());

                    if map.contains_key(&include_key) || map.contains_key(&exclude_key) {
                        let includes = map
                            .get(&include_key)
                            .map(|v| match v {
                                dbt_yaml::Value::String(s, _) if s == "*" || s == "all" => {
                                    Vec::new()
                                } // Empty vec means include all
                                dbt_yaml::Value::Sequence(arr, _) => arr
                                    .iter()
                                    .filter_map(|v| match v {
                                        dbt_yaml::Value::String(s, _) => Some(s.clone()),
                                        _ => None,
                                    })
                                    .collect(),
                                dbt_yaml::Value::String(s, _) => vec![s.clone()],
                                _ => Vec::new(),
                            })
                            .unwrap_or_default(); // Default to empty vec (include all)

                        let excludes = map
                            .get(&exclude_key)
                            .map(|v| match v {
                                dbt_yaml::Value::Sequence(arr, _) => arr
                                    .iter()
                                    .filter_map(|v| match v {
                                        dbt_yaml::Value::String(s, _) => Some(s.clone()),
                                        _ => None,
                                    })
                                    .collect(),
                                dbt_yaml::Value::String(s, _) => vec![s.clone()],
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
/// Returns a Vec of DbtColumn references.
pub fn process_columns(
    columns: Option<&Vec<ColumnProperties>>,
    meta: Option<IndexMap<String, YmlValue>>,
    tags: Option<Vec<String>>,
) -> FsResult<Vec<DbtColumnRef>> {
    Ok(columns
        .map(|cols| {
            // Deduplicate by column name, keeping the last definition for each name.
            // This matches dbt-core/Mantle behaviour where columns are stored in a dict
            // and a later definition silently overwrites an earlier one.
            let mut by_name: IndexMap<String, DbtColumnRef> = IndexMap::new();
            for cp in cols.iter() {
                let (cp_meta, cp_tags, cp_databricks_tags, cp_policy_tags) = cp
                    .config
                    .clone()
                    .map(|c| (c.meta, c.tags, c.databricks_tags, c.policy_tags))
                    .unwrap_or_default();

                let col = Arc::new(DbtColumn {
                    name: cp.name.clone(),
                    data_type: cp.data_type.clone(),
                    description: cp.description.clone(),
                    constraints: cp.constraints.clone().unwrap_or_default(),
                    meta: cp_meta.or_else(|| meta.clone()).unwrap_or_default(),
                    tags: cp_tags
                        .map(|t| t.into())
                        .or_else(|| tags.clone())
                        .unwrap_or_default(),
                    // Top-level policy_tags takes precedence over config.policy_tags
                    policy_tags: cp.policy_tags.clone().or(cp_policy_tags),
                    databricks_tags: cp.databricks_tags.clone().or(cp_databricks_tags),
                    column_mask: cp.column_mask.clone(),
                    quote: cp.quote,
                    deprecated_config: cp.config.clone().unwrap_or_default(),
                });
                by_name.insert(cp.name.clone(), col);
            }
            Ok::<Vec<DbtColumnRef>, Box<dyn std::error::Error>>(by_name.into_values().collect())
        })
        .transpose()?
        .unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_col(name: &str, description: &str) -> ColumnProperties {
        ColumnProperties {
            name: name.to_string(),
            description: Some(description.to_string()),
            data_type: None,
            constraints: None,
            tests: None,
            data_tests: None,
            granularity: None,
            policy_tags: None,
            databricks_tags: None,
            column_mask: None,
            quote: None,
            config: None,
            entity: None,
            dimension: None,
        }
    }

    /// Regression: when the same column name appears multiple times in a YAML schema,
    /// process_columns must deduplicate by name keeping the last definition (matching
    /// dbt-core/Mantle dict semantics). Previously Fusion kept all occurrences, producing
    /// a Vec with duplicate names that caused false state:modified detections.
    #[test]
    fn test_process_columns_deduplicates_by_name_last_wins() {
        let cols = vec![
            make_col("id", "First definition."),
            make_col("name", "The name."),
            make_col("id", "Second definition (last wins)."),
        ];

        let result = process_columns(Some(&cols), None, None).unwrap();

        assert_eq!(result.len(), 2, "duplicate 'id' should be collapsed to one");

        let id_col = result.iter().find(|c| c.name == "id").unwrap();
        assert_eq!(
            id_col.description.as_deref(),
            Some("Second definition (last wins)."),
            "last definition should win"
        );
    }
}

#[derive(UntaggedEnumDeserialize, Serialize, Debug, Clone, DbtSchema, Eq, PartialEq)]
#[serde(untagged)]
pub enum ColumnPropertiesDimension {
    DimensionConfig(ColumnPropertiesDimensionConfig),
    DimensionType(ColumnPropertiesDimensionType),
}

#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema, Eq, PartialEq)]
#[allow(non_camel_case_types)]
pub enum ColumnPropertiesDimensionType {
    categorical,
    time,
}

#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema, Eq, PartialEq)]
pub struct ColumnPropertiesDimensionConfig {
    #[serde(rename = "type")]
    pub type_: ColumnPropertiesDimensionType,
    pub is_partition: Option<bool>,
    pub label: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub config: Option<SemanticLayerElementConfig>,
    pub validity_params: Option<DimensionValidityParams>,
}

#[derive(UntaggedEnumDeserialize, Serialize, Debug, Clone, DbtSchema)]
#[serde(untagged)]
pub enum Entity {
    EntityConfig(EntityConfig),
    EntityType(ColumnPropertiesEntityType),
}

#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema, Eq, PartialEq)]
#[allow(non_camel_case_types)]
pub enum ColumnPropertiesEntityType {
    foreign,
    natural,
    primary,
    unique,
}

#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema)]
pub struct EntityConfig {
    #[serde(rename = "type")]
    pub type_: ColumnPropertiesEntityType,
    pub name: Option<String>,
    pub description: Option<String>,
    pub label: Option<String>,
    pub config: Option<SemanticLayerElementConfig>,
}
