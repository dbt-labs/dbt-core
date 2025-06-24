use std::{collections::BTreeMap, path::PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::skip_serializing_none;

use crate::schemas::{
    common::{DbtQuoting, Expect, FreshnessDefinition, Given, IncludeExclude},
    dbt_column::DbtColumn,
    nodes::TestMetadata,
    project::{
        DataTestConfig, ModelConfig, SeedConfig, SnapshotConfig, SourceConfig, UnitTestConfig,
    },
    properties::ModelConstraint,
    serde::StringOrInteger,
    CommonAttributes, DbtModel, DbtSeed, DbtSnapshot, DbtSource, DbtTest, DbtUnitTest,
    NodeBaseAttributes,
};

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ManifestSeed {
    #[serde(flatten)]
    pub common_attr: CommonAttributes,

    #[serde(flatten)]
    pub base_attr: NodeBaseAttributes,

    // Test Specific Attributes
    pub config: SeedConfig,
    pub root_path: Option<PathBuf>,

    #[serde(flatten)]
    pub other: BTreeMap<String, Value>,
}

impl From<DbtSeed> for ManifestSeed {
    fn from(seed: DbtSeed) -> Self {
        Self {
            common_attr: seed.common_attr,
            base_attr: seed.base_attr,
            config: seed.deprecated_config,
            root_path: seed.root_path,
            other: seed.other,
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ManifestUnitTest {
    #[serde(flatten)]
    pub common_attr: CommonAttributes,

    #[serde(flatten)]
    pub base_attr: NodeBaseAttributes,
    /// Unit Test Specific Attributes
    pub config: UnitTestConfig,
    pub model: String,
    pub given: Vec<Given>,
    pub expect: Expect,
    pub versions: Option<IncludeExclude>,
    pub version: Option<StringOrInteger>,
    pub overrides: Option<Value>,
}

impl From<DbtUnitTest> for ManifestUnitTest {
    fn from(unit_test: DbtUnitTest) -> Self {
        Self {
            common_attr: unit_test.common_attr,
            base_attr: unit_test.base_attr,
            config: unit_test.deprecated_config,
            model: unit_test.model,
            given: unit_test.given,
            expect: unit_test.expect,
            versions: unit_test.versions,
            version: unit_test.version,
            overrides: unit_test.overrides,
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ManifestDataTest {
    #[serde(flatten)]
    pub common_attr: CommonAttributes,
    #[serde(flatten)]
    pub base_attr: NodeBaseAttributes,

    /// Test Specific Attributes
    pub config: DataTestConfig,
    pub column_name: Option<String>,
    pub attached_node: Option<String>,
    pub test_metadata: Option<TestMetadata>,
    pub file_key_name: Option<String>,

    #[serde(flatten)]
    pub other: BTreeMap<String, Value>,
}

impl From<DbtTest> for ManifestDataTest {
    fn from(test: DbtTest) -> Self {
        Self {
            common_attr: test.common_attr,
            base_attr: test.base_attr,
            config: test.deprecated_config,
            column_name: test.column_name,
            attached_node: test.attached_node,
            test_metadata: test.test_metadata,
            file_key_name: test.file_key_name,
            other: test.other,
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ManifestSnapshot {
    #[serde(flatten)]
    pub common_attr: CommonAttributes,
    #[serde(flatten)]
    pub base_attr: NodeBaseAttributes,

    /// Snapshot Specific Attributes
    pub config: SnapshotConfig,

    #[serde(flatten)]
    pub other: BTreeMap<String, Value>,
}

impl From<DbtSnapshot> for ManifestSnapshot {
    fn from(snapshot: DbtSnapshot) -> Self {
        Self {
            common_attr: snapshot.common_attr,
            base_attr: snapshot.base_attr,
            config: snapshot.deprecated_config,
            other: snapshot.other,
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ManifestSource {
    #[serde(flatten)]
    pub common_attr: CommonAttributes,

    // Source Specific Attributes
    pub relation_name: Option<String>,
    pub identifier: String,
    pub source_name: String,
    pub columns: BTreeMap<String, DbtColumn>,
    pub config: SourceConfig,
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

impl From<DbtSource> for ManifestSource {
    fn from(source: DbtSource) -> Self {
        Self {
            common_attr: source.common_attr,
            relation_name: source.relation_name,
            identifier: source.identifier,
            source_name: source.source_name,
            columns: source.columns,
            config: source.deprecated_config,
            quoting: Some(DbtQuoting {
                database: Some(source.quoting.database),
                schema: Some(source.quoting.schema),
                identifier: Some(source.quoting.identifier),
            }),
            source_description: source.source_description,
            unrendered_config: source.unrendered_config,
            unrendered_database: source.unrendered_database,
            unrendered_schema: source.unrendered_schema,
            loader: source.loader,
            loaded_at_field: source.loaded_at_field,
            loaded_at_query: source.loaded_at_query,
            freshness: source.freshness,
            other: source.other,
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ManifestModel {
    #[serde(flatten)]
    pub common_attr: CommonAttributes,

    #[serde(flatten)]
    pub base_attr: NodeBaseAttributes,

    // Model Specific Attributes
    pub config: ModelConfig,
    pub version: Option<StringOrInteger>,
    pub latest_version: Option<StringOrInteger>,
    pub constraints: Option<Vec<ModelConstraint>>,
    pub deprecation_date: Option<String>,
    pub primary_key: Option<Vec<String>>,
    pub time_spine: Option<Value>,

    #[serde(flatten)]
    pub other: BTreeMap<String, Value>,
}

impl From<DbtModel> for ManifestModel {
    fn from(model: DbtModel) -> Self {
        Self {
            common_attr: model.common_attr,
            base_attr: model.base_attr,
            config: model.deprecated_config,
            version: model.version,
            latest_version: model.latest_version,
            constraints: Some(model.constraints),
            deprecation_date: model.deprecation_date,
            primary_key: Some(model.primary_key),
            time_spine: model.time_spine,
            other: model.other,
        }
    }
}
