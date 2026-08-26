use dbt_common::io_args::StaticAnalysisOffReason;
use dbt_yaml::DbtSchema;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::schemas::data_tests::DataTests;
use crate::schemas::dbt_column::ColumnProperties;
use crate::schemas::project::SnapshotConfig;
use crate::schemas::properties::GetConfig;

type YmlValue = dbt_yaml::Value;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema)]
pub struct SnapshotProperties {
    pub name: String,
    pub relation: Option<String>,
    pub columns: Option<Vec<ColumnProperties>>,
    pub config: Option<SnapshotConfig>,
    pub data_tests: Option<Vec<DataTests>>,
    pub description: Option<String>,
    pub meta: Option<IndexMap<String, YmlValue>>,
    #[serde(skip_deserializing, default)]
    pub static_analysis_off_reason: Option<StaticAnalysisOffReason>,
    pub tests: Option<Vec<DataTests>>,
}

impl GetConfig<SnapshotConfig> for SnapshotProperties {
    fn get_config(&self) -> Option<&SnapshotConfig> {
        self.config.as_ref()
    }
}

impl SnapshotProperties {
    pub fn empty(name: String) -> Self {
        Self {
            name,
            relation: None,
            columns: None,
            config: None,
            data_tests: None,
            description: None,
            meta: None,
            static_analysis_off_reason: None,
            tests: None,
        }
    }

    /// Merge legacy top-level snapshot metadata with resolved config metadata.
    /// dbt Core gives `config.meta` precedence when the same key appears in both.
    pub fn merged_meta(
        &self,
        config_meta: Option<IndexMap<String, YmlValue>>,
    ) -> Option<IndexMap<String, YmlValue>> {
        let has_meta = self.meta.is_some() || config_meta.is_some();
        let mut merged = self.meta.clone().unwrap_or_default();
        merged.extend(config_meta.unwrap_or_default());
        has_meta.then_some(merged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_snapshot_meta_merges_with_config_precedence() {
        let properties: SnapshotProperties = dbt_yaml::from_str(
            r#"
name: my_snapshot
meta:
  constraints:
    - name: id_greater_than_zero
      condition: id > 0
  winner: legacy
config:
  meta:
    winner: config
  __warehouse_specific_config__: {}
"#,
        )
        .unwrap();

        let merged = properties.merged_meta(
            properties
                .config
                .as_ref()
                .and_then(|config| config.meta.clone()),
        );

        assert!(merged.as_ref().unwrap().contains_key("constraints"));
        assert_eq!(
            merged
                .as_ref()
                .and_then(|meta| meta.get("winner"))
                .and_then(YmlValue::as_str),
            Some("config")
        );
    }
}
