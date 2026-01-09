use indexmap::IndexMap;
use std::{collections::BTreeMap, sync::Arc};

use dbt_common::tracing::emit::emit_warn_log_message;
use dbt_common::{ErrorCode, FsResult};
use dbt_serde_yaml::{JsonSchema, UntaggedEnumDeserialize};
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::skip_serializing_none;
use strum::Display;

// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;

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
#[derive(Default, Deserialize, Serialize, Debug, Clone, JsonSchema)]
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
    pub quote: Option<bool>,
    pub config: Option<ColumnConfig>,

    pub entity: Option<Entity>,
    pub dimension: Option<ColumnPropertiesDimension>,
}

#[derive(Deserialize, Serialize, Debug, Clone, Default, JsonSchema, Eq, PartialEq, Display)]
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
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, Default, PartialEq, Eq)]
pub struct ColumnConfig {
    #[serde(default)]
    pub tags: Option<StringOrArrayOfStrings>,
    pub meta: Option<IndexMap<String, YmlValue>>,
    pub databricks_tags: Option<BTreeMap<String, YmlValue>>,
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
/// Returns a Vec of DbtColumn references.
///
/// Note: If duplicate column names are present, the last definition wins
/// (matching dbt-core behavior where columns are stored in a dict keyed by name).
pub fn process_columns(
    columns: Option<&Vec<ColumnProperties>>,
    meta: Option<IndexMap<String, YmlValue>>,
    tags: Option<Vec<String>>,
    original_file_path: Option<&str>,
) -> FsResult<Vec<DbtColumnRef>> {
    Ok(columns
        .map(|cols| {
            // Use IndexMap to deduplicate by column name while preserving order.
            // If duplicate names exist, the last definition wins (matches dbt-core behavior).
            let mut column_map: IndexMap<String, DbtColumnRef> = IndexMap::new();

            for cp in cols.iter() {
                let (cp_meta, cp_tags, cp_databricks_tags) = cp
                    .config
                    .clone()
                    .map(|c| (c.meta, c.tags, c.databricks_tags))
                    .unwrap_or_default();

                let column = Arc::new(DbtColumn {
                    name: cp.name.clone(),
                    data_type: cp.data_type.clone(),
                    description: cp.description.clone(),
                    constraints: cp.constraints.clone().unwrap_or_default(),
                    meta: cp_meta.or_else(|| meta.clone()).unwrap_or_default(),
                    tags: cp_tags
                        .map(|t| t.into())
                        .or_else(|| tags.clone())
                        .unwrap_or_default(),
                    policy_tags: cp.policy_tags.clone(),
                    databricks_tags: cp.databricks_tags.clone().or(cp_databricks_tags),
                    quote: cp.quote,
                    deprecated_config: cp.config.clone().unwrap_or_default(),
                });

                if column_map.contains_key(&cp.name) {
                    let location = original_file_path
                        .map(|p| format!(" in '{}'", p))
                        .unwrap_or_default();
                    emit_warn_log_message(
                        ErrorCode::DuplicateColumns,
                        format!("Column '{}' is defined multiple times{}. Only the last definition will be used.", cp.name, location),
                        None,
                    );
                }

                // Insert or overwrite - last definition wins
                column_map.insert(cp.name.clone(), column);
            }

            Ok::<Vec<DbtColumnRef>, Box<dyn std::error::Error>>(column_map.into_values().collect())
        })
        .transpose()?
        .unwrap_or_default())
}

#[derive(UntaggedEnumDeserialize, Serialize, Debug, Clone, JsonSchema, Eq, PartialEq)]
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
    pub validity_params: Option<DimensionValidityParams>,
}

#[derive(UntaggedEnumDeserialize, Serialize, Debug, Clone, JsonSchema)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_columns_deduplicates_by_name_last_wins() {
        // Input: 3 ColumnProperties with duplicate "user_id" names
        // Expected: 2 DbtColumns with "user_id" having the LAST definition's description
        let columns = vec![
            ColumnProperties {
                name: "user_id".to_string(),
                data_type: None,
                description: Some("The primary key for this table".to_string()),
                constraints: None,
                tests: None,
                data_tests: None, // data_tests are not carried to DbtColumn
                granularity: None,
                policy_tags: None,
                databricks_tags: None,
                quote: None,
                config: None,
                entity: None,
                dimension: None,
            },
            ColumnProperties {
                name: "user_id".to_string(), // DUPLICATE
                data_type: None,
                description: Some("Primary key".to_string()), // This should win
                constraints: None,
                tests: None,
                data_tests: None,
                granularity: None,
                policy_tags: None,
                databricks_tags: None,
                quote: None,
                config: None,
                entity: None,
                dimension: None,
            },
            ColumnProperties {
                name: "user_rank".to_string(),
                data_type: None,
                description: Some("The rank of the user".to_string()),
                constraints: None,
                tests: None,
                data_tests: None,
                granularity: None,
                policy_tags: None,
                databricks_tags: None,
                quote: None,
                config: None,
                entity: None,
                dimension: None,
            },
        ];

        let result = process_columns(Some(&columns), None, None, Some("test_schema.yml")).unwrap();

        // Should have 2 columns after deduplication
        assert_eq!(result.len(), 2);

        // First column should be user_id with the LAST definition's description
        assert_eq!(result[0].name, "user_id");
        assert_eq!(result[0].description, Some("Primary key".to_string()));

        // Second column should be user_rank
        assert_eq!(result[1].name, "user_rank");
        assert_eq!(
            result[1].description,
            Some("The rank of the user".to_string())
        );
    }
}
