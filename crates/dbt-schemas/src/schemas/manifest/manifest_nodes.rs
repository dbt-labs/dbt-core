use std::{collections::BTreeMap, path::PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::skip_serializing_none;

use crate::schemas::{
    common::{
        DbtChecksum, DbtContract, DbtQuoting, Expect, FreshnessDefinition, Given, IncludeExclude,
        NodeDependsOn,
    },
    dbt_column::DbtColumn,
    nodes::TestMetadata,
    project::{
        DataTestConfig, ModelConfig, SeedConfig, SnapshotConfig, SourceConfig, UnitTestConfig,
    },
    properties::{ModelConstraint, UnitTestOverrides},
    ref_and_source::{DbtRef, DbtSourceWrapper},
    serde::StringOrInteger,
    DbtModel, DbtSeed, DbtSnapshot, DbtSource, DbtTest, DbtUnitTest,
};

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ManifestCommonAttributes {
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
pub struct ManifestNodeBaseAttributes {
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
pub struct ManifestSeed {
    #[serde(flatten)]
    pub common_attr: ManifestCommonAttributes,

    #[serde(flatten)]
    pub base_attr: ManifestNodeBaseAttributes,

    // Test Specific Attributes
    pub config: SeedConfig,
    pub root_path: Option<PathBuf>,

    #[serde(flatten)]
    pub other: BTreeMap<String, Value>,
}

impl From<DbtSeed> for ManifestSeed {
    fn from(seed: DbtSeed) -> Self {
        Self {
            common_attr: ManifestCommonAttributes {
                unique_id: seed.common_attr.unique_id,
                database: seed.base_attr.database,
                schema: seed.base_attr.schema,
                name: seed.common_attr.name,
                package_name: seed.common_attr.package_name,
                fqn: seed.common_attr.fqn,
                path: seed.common_attr.path,
                original_file_path: seed.common_attr.original_file_path,
                patch_path: seed.common_attr.patch_path,
                description: seed.common_attr.description,
            },
            base_attr: ManifestNodeBaseAttributes {
                alias: seed.base_attr.alias,
                relation_name: seed.base_attr.relation_name,
                columns: seed.base_attr.columns,
                depends_on: seed.base_attr.depends_on,
                refs: seed.base_attr.refs,
                sources: seed.base_attr.sources,
                metrics: seed.base_attr.metrics,
                raw_code: seed.common_attr.raw_code,
                compiled: None,
                compiled_code: None,
                checksum: seed.common_attr.checksum,
                language: seed.common_attr.language,
                unrendered_config: Default::default(),
                doc_blocks: Default::default(),
                extra_ctes_injected: Default::default(),
                extra_ctes: Default::default(),
                created_at: Default::default(),
                compiled_path: Default::default(),
                build_path: Default::default(),
                contract: Default::default(),
            },
            config: seed.deprecated_config,
            root_path: seed.seed_attr.root_path,
            other: seed.other,
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ManifestUnitTest {
    #[serde(flatten)]
    pub common_attr: ManifestCommonAttributes,

    #[serde(flatten)]
    pub base_attr: ManifestNodeBaseAttributes,
    /// Unit Test Specific Attributes
    pub config: UnitTestConfig,
    pub model: String,
    pub given: Vec<Given>,
    pub expect: Expect,
    pub versions: Option<IncludeExclude>,
    pub version: Option<StringOrInteger>,
    pub overrides: Option<UnitTestOverrides>,
}

impl From<DbtUnitTest> for ManifestUnitTest {
    fn from(unit_test: DbtUnitTest) -> Self {
        Self {
            common_attr: ManifestCommonAttributes {
                unique_id: unit_test.common_attr.unique_id,
                database: unit_test.base_attr.database,
                schema: unit_test.base_attr.schema,
                name: unit_test.common_attr.name,
                package_name: unit_test.common_attr.package_name,
                fqn: unit_test.common_attr.fqn,
                path: unit_test.common_attr.path,
                original_file_path: unit_test.common_attr.original_file_path,
                patch_path: unit_test.common_attr.patch_path,
                description: unit_test.common_attr.description,
            },
            base_attr: ManifestNodeBaseAttributes {
                alias: unit_test.base_attr.alias,
                relation_name: unit_test.base_attr.relation_name,
                columns: unit_test.base_attr.columns,
                depends_on: unit_test.base_attr.depends_on,
                refs: unit_test.base_attr.refs,
                sources: unit_test.base_attr.sources,
                metrics: unit_test.base_attr.metrics,
                raw_code: unit_test.common_attr.raw_code,
                compiled: None,
                compiled_code: None,
                checksum: unit_test.common_attr.checksum,
                language: unit_test.common_attr.language,
                unrendered_config: Default::default(),
                doc_blocks: Default::default(),
                extra_ctes_injected: Default::default(),
                extra_ctes: Default::default(),
                created_at: Default::default(),
                compiled_path: Default::default(),
                build_path: Default::default(),
                contract: Default::default(),
            },
            config: unit_test.deprecated_config,
            model: unit_test.unit_test_attr.model,
            given: unit_test.unit_test_attr.given,
            expect: unit_test.unit_test_attr.expect,
            versions: unit_test.unit_test_attr.versions,
            version: unit_test.unit_test_attr.version,
            overrides: unit_test.unit_test_attr.overrides,
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ManifestDataTest {
    #[serde(flatten)]
    pub common_attr: ManifestCommonAttributes,
    #[serde(flatten)]
    pub base_attr: ManifestNodeBaseAttributes,

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
            common_attr: ManifestCommonAttributes {
                unique_id: test.common_attr.unique_id,
                database: test.base_attr.database,
                schema: test.base_attr.schema,
                name: test.common_attr.name,
                package_name: test.common_attr.package_name,
                fqn: test.common_attr.fqn,
                path: test.common_attr.path,
                original_file_path: test.common_attr.original_file_path,
                patch_path: test.common_attr.patch_path,
                description: test.common_attr.description,
            },
            base_attr: ManifestNodeBaseAttributes {
                alias: test.base_attr.alias,
                relation_name: test.base_attr.relation_name,
                columns: test.base_attr.columns,
                depends_on: test.base_attr.depends_on,
                refs: test.base_attr.refs,
                sources: test.base_attr.sources,
                metrics: test.base_attr.metrics,
                raw_code: test.common_attr.raw_code,
                compiled: None,
                compiled_code: None,
                checksum: test.common_attr.checksum,
                language: test.common_attr.language,
                unrendered_config: Default::default(),
                doc_blocks: Default::default(),
                extra_ctes_injected: Default::default(),
                extra_ctes: Default::default(),
                created_at: Default::default(),
                compiled_path: Default::default(),
                build_path: Default::default(),
                contract: Default::default(),
            },
            config: test.deprecated_config,
            column_name: test.test_attr.column_name,
            attached_node: test.test_attr.attached_node,
            test_metadata: test.test_attr.test_metadata,
            file_key_name: test.test_attr.file_key_name,
            other: test.other,
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ManifestSnapshot {
    #[serde(flatten)]
    pub common_attr: ManifestCommonAttributes,
    #[serde(flatten)]
    pub base_attr: ManifestNodeBaseAttributes,

    /// Snapshot Specific Attributes
    pub config: SnapshotConfig,

    #[serde(flatten)]
    pub other: BTreeMap<String, Value>,
}

impl From<DbtSnapshot> for ManifestSnapshot {
    fn from(snapshot: DbtSnapshot) -> Self {
        Self {
            common_attr: ManifestCommonAttributes {
                unique_id: snapshot.common_attr.unique_id,
                database: snapshot.base_attr.database,
                schema: snapshot.base_attr.schema,
                name: snapshot.common_attr.name,
                package_name: snapshot.common_attr.package_name,
                fqn: snapshot.common_attr.fqn,
                path: snapshot.common_attr.path,
                original_file_path: snapshot.common_attr.original_file_path,
                patch_path: snapshot.common_attr.patch_path,
                description: snapshot.common_attr.description,
            },
            base_attr: ManifestNodeBaseAttributes {
                alias: snapshot.base_attr.alias,
                relation_name: snapshot.base_attr.relation_name,
                columns: snapshot.base_attr.columns,
                depends_on: snapshot.base_attr.depends_on,
                refs: snapshot.base_attr.refs,
                sources: snapshot.base_attr.sources,
                metrics: snapshot.base_attr.metrics,
                raw_code: snapshot.common_attr.raw_code,
                compiled: None,
                compiled_code: None,
                checksum: snapshot.common_attr.checksum,
                language: snapshot.common_attr.language,
                unrendered_config: Default::default(),
                doc_blocks: Default::default(),
                extra_ctes_injected: Default::default(),
                extra_ctes: Default::default(),
                created_at: Default::default(),
                compiled_path: Default::default(),
                build_path: Default::default(),
                contract: Default::default(),
            },
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
    pub common_attr: ManifestCommonAttributes,

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
            common_attr: ManifestCommonAttributes {
                unique_id: source.common_attr.unique_id,
                database: source.base_attr.database,
                schema: source.base_attr.schema,
                name: source.common_attr.name,
                package_name: source.common_attr.package_name,
                fqn: source.common_attr.fqn,
                path: source.common_attr.path,
                original_file_path: source.common_attr.original_file_path,
                patch_path: source.common_attr.patch_path,
                description: source.common_attr.description,
            },
            relation_name: source.base_attr.relation_name,
            identifier: source.source_attr.identifier,
            source_name: source.source_attr.source_name,
            columns: source.base_attr.columns,
            config: source.deprecated_config,
            quoting: Some(DbtQuoting {
                database: Some(source.base_attr.quoting.database),
                schema: Some(source.base_attr.quoting.schema),
                identifier: Some(source.base_attr.quoting.identifier),
            }),
            source_description: source.source_attr.source_description,
            unrendered_config: BTreeMap::new(),
            unrendered_database: None,
            unrendered_schema: None,
            loader: source.source_attr.loader,
            loaded_at_field: source.source_attr.loaded_at_field,
            loaded_at_query: source.source_attr.loaded_at_query,
            freshness: source.source_attr.freshness,
            other: source.other,
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ManifestModel {
    #[serde(flatten)]
    pub common_attr: ManifestCommonAttributes,

    #[serde(flatten)]
    pub base_attr: ManifestNodeBaseAttributes,

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
            common_attr: ManifestCommonAttributes {
                unique_id: model.common_attr.unique_id,
                database: model.base_attr.database,
                schema: model.base_attr.schema,
                name: model.common_attr.name,
                package_name: model.common_attr.package_name,
                fqn: model.common_attr.fqn,
                path: model.common_attr.path,
                original_file_path: model.common_attr.original_file_path,
                patch_path: model.common_attr.patch_path,
                description: model.common_attr.description,
            },
            base_attr: ManifestNodeBaseAttributes {
                alias: model.base_attr.alias,
                relation_name: model.base_attr.relation_name,
                columns: model.base_attr.columns,
                depends_on: model.base_attr.depends_on,
                refs: model.base_attr.refs,
                sources: model.base_attr.sources,
                metrics: model.base_attr.metrics,
                raw_code: model.common_attr.raw_code,
                compiled: None,
                compiled_code: None,
                checksum: model.common_attr.checksum,
                language: model.common_attr.language,
                unrendered_config: Default::default(),
                doc_blocks: Default::default(),
                extra_ctes_injected: Default::default(),
                extra_ctes: Default::default(),
                created_at: Default::default(),
                compiled_path: Default::default(),
                build_path: Default::default(),
                contract: Default::default(),
            },
            config: model.deprecated_config,
            version: model.model_attr.version,
            latest_version: model.model_attr.latest_version,
            constraints: Some(model.model_attr.constraints),
            deprecation_date: model.model_attr.deprecation_date,
            primary_key: Some(model.model_attr.primary_key),
            time_spine: model.model_attr.time_spine,
            other: model.other,
        }
    }
}
