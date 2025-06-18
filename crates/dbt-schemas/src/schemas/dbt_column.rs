use std::collections::{BTreeMap, HashMap};

use dbt_common::{FsError, FsResult};
use dbt_serde_yaml::{JsonSchema, Verbatim};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::skip_serializing_none;

use super::{
    common::Constraint,
    data_tests::DataTests,
    manifest::DbtConfig,
    serde::{BooleanOrJinjaString, StringOrArrayOfStrings},
};

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtColumn {
    pub name: String,
    pub data_type: Option<String>,
    pub description: Option<String>,
    pub constraints: Vec<Constraint>,
    // Note: DbtConfig is ginormous, so we put it on the heap to save some memory:
    pub config: Option<Box<DbtConfig>>,
    pub policy_tags: Option<Vec<String>>,
    pub quote: Option<bool>,
}

impl TryFrom<ColumnProperties> for DbtColumn {
    type Error = Box<FsError>;

    fn try_from(value: ColumnProperties) -> Result<Self, Self::Error> {
        let constraints = value.constraints.unwrap_or_default();

        // Convert the column config to DbtConfig if it exists
        let config = value.config.map(|c| {
            let mut dbt_config = DbtConfig::default();
            if let Some(meta) = c.meta {
                dbt_config.meta = Some(meta);
            }
            if let Some(tags) = c.tags {
                dbt_config.tags = match tags {
                    StringOrArrayOfStrings::String(s) => Some(vec![s]),
                    StringOrArrayOfStrings::ArrayOfStrings(v) => Some(v),
                };
            }
            dbt_config
        });

        Ok(DbtColumn {
            name: value.name,
            data_type: value.data_type,
            description: value.description,
            constraints,
            policy_tags: value.policy_tags,
            config: config.map(Box::new),
            quote: value.quote.map(bool::from),
        })
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ColumnProperties {
    pub name: String,
    pub data_type: Option<String>,
    pub description: Option<String>,
    pub constraints: Option<Vec<Constraint>>,
    pub data_tests: Verbatim<Option<Vec<DataTests>>>,
    pub granularity: Option<ColumnPropertiesGranularity>,
    pub policy_tags: Option<Vec<String>>,
    pub quote: Option<BooleanOrJinjaString>,
    pub tests: Verbatim<Option<Vec<DataTests>>>,
    pub config: Option<ColumnConfig>,
}

#[derive(Deserialize, Serialize, Debug, Clone, Default, JsonSchema)]
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
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, Default)]
pub struct ColumnConfig {
    #[serde(flatten)]
    pub additional_properties: HashMap<String, Value>,
    pub tags: Option<StringOrArrayOfStrings>,
    pub meta: Option<BTreeMap<String, Value>>,
}

/// Process columns by merging parent config with each column's config.
/// Returns a BTreeMap of column name to DbtColumn.
pub fn process_columns(
    columns: Option<&Vec<ColumnProperties>>,
    parent_config: &DbtConfig,
) -> FsResult<BTreeMap<String, DbtColumn>> {
    Ok(columns
        .map(|cols| {
            cols.iter()
                .map(|cp| {
                    let mut dbt_col: DbtColumn = cp.clone().try_into()?;
                    if let Some(config) = &mut dbt_col.config {
                        config.default_to(parent_config);
                    } else {
                        dbt_col.config = Some(Box::new(parent_config.clone()));
                    }
                    Ok(dbt_col)
                })
                .collect::<Result<Vec<DbtColumn>, Box<dyn std::error::Error>>>()
        })
        .transpose()?
        .map(|cols| {
            cols.into_iter()
                .map(|c| (c.name.clone(), c))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default())
}
