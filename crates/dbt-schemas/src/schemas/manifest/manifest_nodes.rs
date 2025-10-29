use std::{collections::BTreeMap, path::PathBuf};

use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;

use crate::schemas::{
    DbtAnalysis, DbtExposure, DbtFunction, DbtModel, DbtSeed, DbtSnapshot, DbtSource, DbtTest,
    DbtUnitTest,
    common::{
        Access, DbtChecksum, DbtContract, DbtMaterialization, DbtQuoting, Expect,
        FreshnessDefinition, Given, IncludeExclude, NodeDependsOn, PersistDocsConfig,
    },
    dbt_column::{DbtColumnRef, deserialize_dbt_columns, serialize_dbt_columns},
    manifest::{
        DbtMetric, DbtOperation, DbtSavedQuery, DbtSemanticModel,
        common::{DbtOwner, SourceFileMetadata, WhereFilterIntersection},
        metric::{MeasureAggregationParameters, MetricTypeParams, NonAdditiveDimension},
        semantic_model::{NodeRelation, SemanticEntity, SemanticMeasure, SemanticModelDefaults},
    },
    nodes::TestMetadata,
    project::{
        AnalysesConfig, DataTestConfig, ExposureConfig, FunctionConfig, MetricConfig, ModelConfig,
        SavedQueryConfig, SeedConfig, SemanticModelConfig, SnapshotConfig, SourceConfig,
        UnitTestConfig,
    },
    properties::{
        ModelConstraint, UnitTestOverrides,
        metrics_properties::{AggregationType, MetricType},
        model_properties::ModelPropertiesTimeSpine,
    },
    ref_and_source::{DbtRef, DbtSourceWrapper},
    semantic_layer::semantic_manifest::SemanticLayerElementConfig,
    serde::{StringOrArrayOfStrings, StringOrInteger},
};

use dbt_common::io_args::StaticAnalysisKind;

fn default_analysis_materialized() -> DbtMaterialization {
    DbtMaterialization::Analysis
}

fn default_analysis_static_analysis() -> StaticAnalysisKind {
    StaticAnalysisKind::Off
}

fn default_analysis_enabled() -> bool {
    true
}

/// Common attributes for all manifest nodes, materializable or not.
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ManifestCommonAttributes {
    // Identifiers
    pub unique_id: String,
    pub name: String,
    pub package_name: String,
    pub fqn: Vec<String>,

    // Paths
    pub path: PathBuf,
    pub original_file_path: PathBuf,

    // Meta
    pub description: Option<String>,

    #[serde(default)]
    pub tags: Vec<String>,

    #[serde(default)]
    pub meta: BTreeMap<String, YmlValue>,
}

/// Common attributes for materializable nodes, i.e. models, sources, snapshots, tests, etc.
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ManifestMaterializableCommonAttributes {
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
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub meta: BTreeMap<String, YmlValue>,
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
    #[serde(
        default,
        serialize_with = "serialize_dbt_columns",
        deserialize_with = "deserialize_dbt_columns"
    )]
    pub columns: Vec<DbtColumnRef>,
    pub depends_on: NodeDependsOn,
    #[serde(default)]
    pub refs: Vec<DbtRef>,
    #[serde(default)]
    pub sources: Vec<DbtSourceWrapper>,
    #[serde(default)]
    pub functions: Vec<DbtRef>,

    // Code
    pub raw_code: Option<String>,
    pub compiled: Option<bool>,
    pub compiled_code: Option<String>,
    #[serde(default)]
    pub unrendered_config: BTreeMap<String, YmlValue>,

    // Metadata
    pub doc_blocks: Option<Vec<YmlValue>>,
    pub extra_ctes_injected: Option<bool>,
    pub extra_ctes: Option<Vec<YmlValue>>,
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
    pub __common_attr__: ManifestMaterializableCommonAttributes,

    pub __base_attr__: ManifestNodeBaseAttributes,

    // Test Specific Attributes
    pub config: SeedConfig,
    pub root_path: Option<PathBuf>,

    pub __other__: BTreeMap<String, YmlValue>,
}

impl From<DbtSeed> for ManifestSeed {
    fn from(seed: DbtSeed) -> Self {
        Self {
            __common_attr__: ManifestMaterializableCommonAttributes {
                unique_id: seed.__common_attr__.unique_id,
                database: seed.__base_attr__.database,
                schema: seed.__base_attr__.schema,
                name: seed.__common_attr__.name,
                package_name: seed.__common_attr__.package_name,
                fqn: seed.__common_attr__.fqn,
                path: seed.__common_attr__.path,
                original_file_path: seed.__common_attr__.original_file_path,
                patch_path: seed.__common_attr__.patch_path,
                description: seed.__common_attr__.description,
                tags: seed.__common_attr__.tags,
                meta: seed.__common_attr__.meta,
            },
            __base_attr__: ManifestNodeBaseAttributes {
                alias: seed.__base_attr__.alias,
                relation_name: seed.__base_attr__.relation_name,
                columns: seed.__base_attr__.columns,
                depends_on: seed.__base_attr__.depends_on,
                refs: seed.__base_attr__.refs,
                sources: seed.__base_attr__.sources,
                functions: seed.__base_attr__.functions,
                metrics: seed.__base_attr__.metrics,
                raw_code: seed.__common_attr__.raw_code,
                compiled: None,
                compiled_code: None,
                checksum: seed.__common_attr__.checksum,
                language: seed.__common_attr__.language,
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
            root_path: seed.__seed_attr__.root_path,
            __other__: seed.__other__,
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ManifestUnitTest {
    pub __common_attr__: ManifestMaterializableCommonAttributes,

    pub __base_attr__: ManifestNodeBaseAttributes,
    /// Unit Test Specific Attributes
    pub config: UnitTestConfig,
    pub model: String,
    pub given: Vec<Given>,
    pub expect: Expect,
    pub versions: Option<IncludeExclude>,
    pub version: Option<StringOrInteger>,
    pub overrides: Option<UnitTestOverrides>,
    #[serde(rename = "_event_status")]
    pub field_event_status: Option<BTreeMap<String, YmlValue>>,
    #[serde(rename = "_pre_injected_sql")]
    pub field_pre_injected_sql: Option<String>,
    pub tested_node_unique_id: Option<String>,
    pub this_input_node_unique_id: Option<String>,
}

impl From<DbtUnitTest> for ManifestUnitTest {
    fn from(unit_test: DbtUnitTest) -> Self {
        Self {
            __common_attr__: ManifestMaterializableCommonAttributes {
                unique_id: unit_test.__common_attr__.unique_id,
                database: unit_test.__base_attr__.database,
                schema: unit_test.__base_attr__.schema,
                name: unit_test.__common_attr__.name,
                package_name: unit_test.__common_attr__.package_name,
                fqn: unit_test.__common_attr__.fqn,
                path: unit_test.__common_attr__.path,
                original_file_path: unit_test.__common_attr__.original_file_path,
                patch_path: unit_test.__common_attr__.patch_path,
                description: unit_test.__common_attr__.description,
                tags: unit_test.__common_attr__.tags,
                meta: unit_test.__common_attr__.meta,
            },
            __base_attr__: ManifestNodeBaseAttributes {
                alias: unit_test.__base_attr__.alias,
                relation_name: unit_test.__base_attr__.relation_name,
                columns: unit_test.__base_attr__.columns,
                depends_on: unit_test.__base_attr__.depends_on,
                refs: unit_test.__base_attr__.refs,
                sources: unit_test.__base_attr__.sources,
                functions: unit_test.__base_attr__.functions,
                metrics: unit_test.__base_attr__.metrics,
                raw_code: unit_test.__common_attr__.raw_code,
                compiled: None,
                compiled_code: None,
                checksum: unit_test.__common_attr__.checksum,
                language: unit_test.__common_attr__.language,
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
            model: unit_test.__unit_test_attr__.model,
            given: unit_test.__unit_test_attr__.given,
            expect: unit_test.__unit_test_attr__.expect,
            versions: unit_test.__unit_test_attr__.versions,
            version: unit_test.__unit_test_attr__.version,
            overrides: unit_test.__unit_test_attr__.overrides,
            field_event_status: unit_test.field_event_status,
            field_pre_injected_sql: unit_test.field_pre_injected_sql,
            tested_node_unique_id: unit_test.tested_node_unique_id,
            this_input_node_unique_id: unit_test.this_input_node_unique_id,
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ManifestDataTest {
    pub __common_attr__: ManifestMaterializableCommonAttributes,
    pub __base_attr__: ManifestNodeBaseAttributes,

    /// Test Specific Attributes
    pub config: DataTestConfig,
    pub column_name: Option<String>,
    pub attached_node: Option<String>,
    pub test_metadata: Option<TestMetadata>,
    pub file_key_name: Option<String>,
    pub generated_sql_file: Option<String>,

    pub __other__: BTreeMap<String, YmlValue>,
}

impl From<DbtTest> for ManifestDataTest {
    fn from(test: DbtTest) -> Self {
        Self {
            __common_attr__: ManifestMaterializableCommonAttributes {
                unique_id: test.__common_attr__.unique_id,
                database: test.__base_attr__.database,
                schema: test.__base_attr__.schema,
                name: test.__common_attr__.name,
                package_name: test.__common_attr__.package_name,
                fqn: test.__common_attr__.fqn,
                path: test.__common_attr__.path,

                // NOTE: `test.__common_attr__.original_file_path` is the path
                // to the generated SQL file, which is *not* what we want here
                // -- in the manifest, `original_file_path` should be the path
                // to the YAML file where the test was defined
                original_file_path: test.manifest_original_file_path,

                patch_path: test.__common_attr__.patch_path,

                description: test.__common_attr__.description,
                tags: test.__common_attr__.tags,
                meta: test.__common_attr__.meta,
            },
            __base_attr__: ManifestNodeBaseAttributes {
                alias: test.__base_attr__.alias,
                relation_name: test.__base_attr__.relation_name,
                columns: test.__base_attr__.columns,
                depends_on: test.__base_attr__.depends_on,
                refs: test.__base_attr__.refs,
                sources: test.__base_attr__.sources,
                functions: test.__base_attr__.functions,
                metrics: test.__base_attr__.metrics,
                raw_code: test.__common_attr__.raw_code,
                compiled: None,
                compiled_code: None,
                checksum: test.__common_attr__.checksum,
                language: test.__common_attr__.language,
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
            column_name: test.__test_attr__.column_name,
            attached_node: test.__test_attr__.attached_node,
            test_metadata: test.__test_attr__.test_metadata,
            file_key_name: test.__test_attr__.file_key_name,
            generated_sql_file: Some(
                test.__common_attr__
                    .original_file_path
                    .to_string_lossy()
                    .to_string(),
            ),
            __other__: test.__other__,
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ManifestSnapshot {
    pub __common_attr__: ManifestMaterializableCommonAttributes,
    pub __base_attr__: ManifestNodeBaseAttributes,

    /// Snapshot Specific Attributes
    pub config: SnapshotConfig,

    pub __other__: BTreeMap<String, YmlValue>,
}

impl From<DbtSnapshot> for ManifestSnapshot {
    fn from(snapshot: DbtSnapshot) -> Self {
        Self {
            __common_attr__: ManifestMaterializableCommonAttributes {
                unique_id: snapshot.__common_attr__.unique_id,
                database: snapshot.__base_attr__.database,
                schema: snapshot.__base_attr__.schema,
                name: snapshot.__common_attr__.name,
                package_name: snapshot.__common_attr__.package_name,
                fqn: snapshot.__common_attr__.fqn,
                path: snapshot.__common_attr__.path,
                original_file_path: snapshot.__common_attr__.original_file_path,
                patch_path: snapshot.__common_attr__.patch_path,
                description: snapshot.__common_attr__.description,
                tags: snapshot.__common_attr__.tags,
                meta: snapshot.__common_attr__.meta,
            },
            __base_attr__: ManifestNodeBaseAttributes {
                alias: snapshot.__base_attr__.alias,
                relation_name: snapshot.__base_attr__.relation_name,
                columns: snapshot.__base_attr__.columns,
                depends_on: snapshot.__base_attr__.depends_on,
                refs: snapshot.__base_attr__.refs,
                sources: snapshot.__base_attr__.sources,
                functions: snapshot.__base_attr__.functions,
                metrics: snapshot.__base_attr__.metrics,
                raw_code: snapshot.__common_attr__.raw_code,
                compiled: None,
                compiled_code: None,
                checksum: snapshot.__common_attr__.checksum,
                language: snapshot.__common_attr__.language,
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
            __other__: snapshot.__other__,
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ManifestSource {
    pub __common_attr__: ManifestMaterializableCommonAttributes,

    // Source Specific Attributes
    pub relation_name: Option<String>,
    pub identifier: String,
    pub source_name: String,
    #[serde(
        serialize_with = "serialize_dbt_columns",
        deserialize_with = "deserialize_dbt_columns"
    )]
    pub columns: Vec<DbtColumnRef>,
    pub config: SourceConfig,
    pub quoting: Option<DbtQuoting>,
    pub source_description: String,
    pub unrendered_config: BTreeMap<String, YmlValue>,
    pub unrendered_database: Option<String>,
    pub unrendered_schema: Option<String>,
    #[serde(default)]
    pub loader: String,
    pub loaded_at_field: Option<String>,
    pub loaded_at_query: Option<String>,

    #[serialize_always]
    pub freshness: Option<FreshnessDefinition>,

    pub __other__: BTreeMap<String, YmlValue>,
}

impl From<DbtSource> for ManifestSource {
    fn from(source: DbtSource) -> Self {
        Self {
            __common_attr__: ManifestMaterializableCommonAttributes {
                unique_id: source.__common_attr__.unique_id,
                database: source.__base_attr__.database,
                schema: source.__base_attr__.schema,
                name: source.__common_attr__.name,
                package_name: source.__common_attr__.package_name,
                fqn: source.__common_attr__.fqn,
                path: source.__common_attr__.path,
                original_file_path: source.__common_attr__.original_file_path,
                patch_path: source.__common_attr__.patch_path,
                description: source.__common_attr__.description,
                tags: source.__common_attr__.tags,
                meta: source.__common_attr__.meta,
            },
            relation_name: source.__base_attr__.relation_name,
            identifier: source.__source_attr__.identifier,
            source_name: source.__source_attr__.source_name,
            columns: source.__base_attr__.columns,
            config: source.deprecated_config,
            quoting: Some(DbtQuoting {
                database: Some(source.__base_attr__.quoting.database),
                schema: Some(source.__base_attr__.quoting.schema),
                identifier: Some(source.__base_attr__.quoting.identifier),
                snowflake_ignore_case: None,
            }),
            source_description: source.__source_attr__.source_description,
            unrendered_config: BTreeMap::new(),
            unrendered_database: None,
            unrendered_schema: None,
            loader: source.__source_attr__.loader,
            loaded_at_field: source.__source_attr__.loaded_at_field,
            loaded_at_query: source.__source_attr__.loaded_at_query,
            freshness: source.__source_attr__.freshness,
            __other__: source.__other__,
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ManifestModel {
    pub __common_attr__: ManifestMaterializableCommonAttributes,

    pub __base_attr__: ManifestNodeBaseAttributes,

    // Model Specific Attributes
    pub access: Option<Access>,
    pub group: Option<String>,
    pub config: ModelConfig,
    pub version: Option<StringOrInteger>,
    pub latest_version: Option<StringOrInteger>,
    pub constraints: Option<Vec<ModelConstraint>>,
    pub deprecation_date: Option<String>,
    pub primary_key: Option<Vec<String>>,
    pub time_spine: Option<ModelPropertiesTimeSpine>,

    pub __other__: BTreeMap<String, YmlValue>,
}

impl From<DbtModel> for ManifestModel {
    fn from(model: DbtModel) -> Self {
        Self {
            __common_attr__: ManifestMaterializableCommonAttributes {
                unique_id: model.__common_attr__.unique_id,
                database: model.__base_attr__.database,
                schema: model.__base_attr__.schema,
                name: model.__common_attr__.name,
                package_name: model.__common_attr__.package_name,
                fqn: model.__common_attr__.fqn,
                path: model.__common_attr__.path,
                original_file_path: model.__common_attr__.original_file_path,
                patch_path: model.__common_attr__.patch_path,
                description: model.__common_attr__.description,
                tags: model.__common_attr__.tags,
                meta: model.__common_attr__.meta,
            },
            __base_attr__: ManifestNodeBaseAttributes {
                alias: model.__base_attr__.alias,
                relation_name: model.__base_attr__.relation_name,
                columns: model.__base_attr__.columns,
                depends_on: model.__base_attr__.depends_on,
                refs: model.__base_attr__.refs,
                sources: model.__base_attr__.sources,
                functions: model.__base_attr__.functions,
                metrics: model.__base_attr__.metrics,
                raw_code: model.__common_attr__.raw_code,
                compiled: None,
                compiled_code: None,
                checksum: model.__common_attr__.checksum,
                language: model.__common_attr__.language,
                unrendered_config: Default::default(),
                doc_blocks: Default::default(),
                extra_ctes_injected: Default::default(),
                extra_ctes: Default::default(),
                created_at: Default::default(),
                compiled_path: Default::default(),
                build_path: Default::default(),
                contract: model.__model_attr__.contract.unwrap_or_default(),
            },
            access: Some(model.__model_attr__.access),
            group: model.__model_attr__.group,
            config: model.deprecated_config,
            version: model.__model_attr__.version,
            latest_version: model.__model_attr__.latest_version,
            constraints: Some(model.__model_attr__.constraints),
            deprecation_date: model.__model_attr__.deprecation_date,
            primary_key: Some(model.__model_attr__.primary_key),
            time_spine: model
                .__model_attr__
                .time_spine
                .map(|ts| ModelPropertiesTimeSpine {
                    custom_granularities: Some(ts.custom_granularities),
                    standard_granularity_column: ts.primary_column.name,
                }),
            __other__: model.__other__,
        }
    }
}
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ManifestAnalysis {
    pub __common_attr__: ManifestMaterializableCommonAttributes,

    pub __base_attr__: ManifestNodeBaseAttributes,

    #[serde(default = "default_analysis_materialized")]
    pub materialized: DbtMaterialization,
    #[serde(default = "default_analysis_static_analysis")]
    pub static_analysis: StaticAnalysisKind,
    #[serde(default = "default_analysis_enabled")]
    pub enabled: bool,
    pub quoting: Option<DbtQuoting>,
    #[serde(default)]
    pub quoting_ignore_case: bool,
    pub persist_docs: Option<PersistDocsConfig>,
    pub config: AnalysesConfig,

    pub __other__: BTreeMap<String, YmlValue>,
}

impl From<DbtAnalysis> for ManifestAnalysis {
    fn from(analysis: DbtAnalysis) -> Self {
        Self {
            __common_attr__: ManifestMaterializableCommonAttributes {
                unique_id: analysis.__common_attr__.unique_id,
                database: analysis.__base_attr__.database,
                schema: analysis.__base_attr__.schema,
                name: analysis.__common_attr__.name,
                package_name: analysis.__common_attr__.package_name,
                fqn: analysis.__common_attr__.fqn,
                path: analysis.__common_attr__.path,
                original_file_path: analysis.__common_attr__.original_file_path,
                patch_path: analysis.__common_attr__.patch_path,
                description: analysis.__common_attr__.description,
                tags: analysis.__common_attr__.tags,
                meta: analysis.__common_attr__.meta,
            },
            __base_attr__: ManifestNodeBaseAttributes {
                alias: analysis.__base_attr__.alias,
                relation_name: analysis.__base_attr__.relation_name,
                columns: analysis.__base_attr__.columns,
                depends_on: analysis.__base_attr__.depends_on,
                refs: analysis.__base_attr__.refs,
                sources: analysis.__base_attr__.sources,
                metrics: analysis.__base_attr__.metrics,
                raw_code: analysis.__common_attr__.raw_code,
                compiled: None,
                compiled_code: None,
                checksum: analysis.__common_attr__.checksum,
                language: analysis.__common_attr__.language,
                unrendered_config: Default::default(),
                doc_blocks: Default::default(),
                extra_ctes_injected: Default::default(),
                extra_ctes: Default::default(),
                created_at: Default::default(),
                compiled_path: Default::default(),
                build_path: Default::default(),
                contract: Default::default(),
                functions: analysis.__base_attr__.functions,
            },
            materialized: analysis.__base_attr__.materialized,
            static_analysis: analysis.__base_attr__.static_analysis.into_inner(),
            enabled: analysis.__base_attr__.enabled,
            quoting: Some(DbtQuoting {
                database: Some(analysis.__base_attr__.quoting.database),
                identifier: Some(analysis.__base_attr__.quoting.identifier),
                schema: Some(analysis.__base_attr__.quoting.schema),
                snowflake_ignore_case: None,
            }),
            quoting_ignore_case: analysis.__base_attr__.quoting_ignore_case,
            persist_docs: analysis.__base_attr__.persist_docs.clone(),
            config: analysis.deprecated_config,
            __other__: analysis.__other__,
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ManifestOperation {
    pub __common_attr__: ManifestMaterializableCommonAttributes,

    pub __base_attr__: ManifestNodeBaseAttributes,

    pub __other__: BTreeMap<String, YmlValue>,
}

impl From<DbtOperation> for ManifestOperation {
    fn from(operation: DbtOperation) -> Self {
        Self {
            __common_attr__: ManifestMaterializableCommonAttributes {
                unique_id: operation.__common_attr__.unique_id,
                name: operation.__common_attr__.name,
                package_name: operation.__common_attr__.package_name,
                fqn: operation.__common_attr__.fqn,
                path: operation.__common_attr__.path,
                original_file_path: operation.__common_attr__.original_file_path,
                patch_path: operation.__common_attr__.patch_path,
                description: operation.__common_attr__.description,
                ..Default::default()
            },
            __base_attr__: ManifestNodeBaseAttributes {
                alias: operation.__base_attr__.alias,
                relation_name: operation.__base_attr__.relation_name,
                columns: operation.__base_attr__.columns,
                depends_on: operation.__base_attr__.depends_on,
                refs: operation.__base_attr__.refs,
                sources: operation.__base_attr__.sources,
                functions: operation.__base_attr__.functions,
                metrics: operation.__base_attr__.metrics,
                raw_code: operation.__common_attr__.raw_code,
                compiled: None,
                compiled_code: None,
                checksum: operation.__common_attr__.checksum,
                language: operation.__common_attr__.language,
                ..Default::default()
            },
            __other__: operation.__other__,
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ManifestFunction {
    pub __common_attr__: ManifestMaterializableCommonAttributes,

    pub __base_attr__: ManifestNodeBaseAttributes,

    // Function Specific Attributes
    pub config: FunctionConfig,
    pub access: Access,
    pub group: Option<String>,
    pub language: Option<String>,
    pub on_configuration_change: Option<String>,
    pub returns: Option<crate::schemas::properties::FunctionReturnType>,
    pub arguments: Option<Vec<crate::schemas::properties::FunctionArgument>>,
    #[serde(rename = "type")]
    pub function_kind: Option<crate::schemas::properties::FunctionKind>,

    pub __other__: BTreeMap<String, YmlValue>,
}

impl From<DbtFunction> for ManifestFunction {
    fn from(function: DbtFunction) -> Self {
        Self {
            __common_attr__: ManifestMaterializableCommonAttributes {
                database: function.__base_attr__.database,
                schema: function.__base_attr__.schema,
                unique_id: function.__common_attr__.unique_id,
                name: function.__common_attr__.name,
                package_name: function.__common_attr__.package_name,
                fqn: function.__common_attr__.fqn,
                path: function.__common_attr__.original_file_path.clone(),
                patch_path: function.__common_attr__.patch_path,
                original_file_path: function.__common_attr__.original_file_path,
                description: function.__common_attr__.description,
                tags: function.__common_attr__.tags,
                meta: function.__common_attr__.meta,
            },
            __base_attr__: ManifestNodeBaseAttributes {
                alias: function.__base_attr__.alias,
                relation_name: function.__base_attr__.relation_name,
                compiled_path: None,
                build_path: None,
                columns: Vec::new(),
                depends_on: function.__base_attr__.depends_on,
                refs: function.__base_attr__.refs,
                sources: function.__base_attr__.sources,
                raw_code: function.__common_attr__.raw_code,
                compiled: None,
                ..Default::default()
            },
            config: function.deprecated_config,
            access: function.__function_attr__.access,
            group: function.__function_attr__.group,
            language: function.__function_attr__.language,
            on_configuration_change: function.__function_attr__.on_configuration_change,
            returns: function.__function_attr__.returns,
            arguments: function.__function_attr__.arguments,
            function_kind: Some(function.__function_attr__.function_kind),
            __other__: function.__other__,
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ManifestExposureNodeBaseAttributes {
    // Derived
    pub depends_on: NodeDependsOn,
    #[serde(default)]
    pub refs: Vec<DbtRef>,
    #[serde(default)]
    pub sources: Vec<DbtSourceWrapper>,

    #[serde(default)]
    pub unrendered_config: BTreeMap<String, YmlValue>,

    // Metadata
    #[serde(default)]
    pub metrics: Vec<Vec<String>>,
    #[serde(default)]
    pub created_at: Option<f64>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ManifestExposure {
    pub __common_attr__: ManifestCommonAttributes,

    pub __base_attr__: ManifestExposureNodeBaseAttributes,

    // Exposure Specific Attributes
    pub owner: DbtOwner,
    pub label: Option<String>,
    pub maturity: Option<String>,
    #[serde(rename = "type")]
    pub type_: String,
    pub url: Option<String>,
    pub config: ExposureConfig,

    pub __other__: BTreeMap<String, YmlValue>,
}

impl From<DbtExposure> for ManifestExposure {
    fn from(exposure: DbtExposure) -> Self {
        Self {
            __common_attr__: ManifestCommonAttributes {
                unique_id: exposure.__common_attr__.unique_id,
                name: exposure.__common_attr__.name,
                package_name: exposure.__common_attr__.package_name,
                fqn: exposure.__common_attr__.fqn,
                path: exposure.__common_attr__.path,
                original_file_path: exposure.__common_attr__.original_file_path,
                description: exposure.__common_attr__.description,
                tags: exposure.__common_attr__.tags,
                meta: exposure.__common_attr__.meta,
            },
            __base_attr__: ManifestExposureNodeBaseAttributes {
                depends_on: exposure.__base_attr__.depends_on,
                refs: exposure.__base_attr__.refs,
                sources: exposure.__base_attr__.sources,
                metrics: exposure.__base_attr__.metrics,
                unrendered_config: exposure.__exposure_attr__.unrendered_config,
                created_at: None,
            },
            owner: exposure.__exposure_attr__.owner,
            label: exposure.__exposure_attr__.label,
            maturity: exposure.__exposure_attr__.maturity,
            type_: exposure.__exposure_attr__.type_,
            url: exposure.__exposure_attr__.url,
            config: exposure.deprecated_config,
            __other__: Default::default(),
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ManifestMetricNodeBaseAttributes {
    // Derived
    pub depends_on: NodeDependsOn,

    #[serde(default)]
    pub refs: Vec<DbtRef>,

    #[serde(default)]
    pub sources: Vec<DbtSourceWrapper>,

    #[serde(default)]
    pub unrendered_config: BTreeMap<String, YmlValue>,

    #[serde(default)]
    pub created_at: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ManifestMetric {
    pub __common_attr__: ManifestCommonAttributes,
    pub __base_attr__: ManifestMetricNodeBaseAttributes,

    // Metric Specific Attributes
    pub label: String,
    #[serde(rename = "type")]
    pub metric_type: MetricType,
    pub type_params: MetricTypeParams,
    pub filter: Option<WhereFilterIntersection>,
    pub metadata: Option<SourceFileMetadata>,
    pub time_granularity: Option<String>,
    pub group: Option<String>,

    pub config: ManifestMetricConfig,

    #[serde(default)]
    pub metrics: Vec<Vec<String>>,

    pub __other__: BTreeMap<String, YmlValue>,
}

impl From<DbtMetric> for ManifestMetric {
    fn from(metric: DbtMetric) -> Self {
        Self {
            __common_attr__: ManifestCommonAttributes {
                unique_id: metric.__common_attr__.unique_id,
                name: metric.__common_attr__.name,
                package_name: metric.__common_attr__.package_name,
                fqn: metric.__common_attr__.fqn,
                path: metric.__common_attr__.path,
                original_file_path: metric.__common_attr__.original_file_path,
                description: metric.__common_attr__.description,
                tags: metric.__common_attr__.tags,
                meta: metric.__common_attr__.meta,
            },
            __base_attr__: ManifestMetricNodeBaseAttributes {
                depends_on: metric.__base_attr__.depends_on,
                refs: metric.__base_attr__.refs,
                sources: metric.__base_attr__.sources,
                unrendered_config: metric.__metric_attr__.unrendered_config,
                created_at: metric.__metric_attr__.created_at,
            },
            label: metric.__metric_attr__.label.unwrap_or_default(),
            metric_type: metric.__metric_attr__.metric_type,
            type_params: metric.__metric_attr__.type_params,
            filter: metric.__metric_attr__.filter,
            metadata: metric.__metric_attr__.metadata,
            time_granularity: metric.__metric_attr__.time_granularity.clone(),
            group: metric.__metric_attr__.group.clone(),
            config: metric.deprecated_config.into(),
            __other__: metric.__other__,
            metrics: vec![], // TODO: metric.__metric_attr__.metrics.clone(),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ManifestMetricConfig {
    pub enabled: bool,

    pub meta: Option<BTreeMap<String, YmlValue>>,

    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub tags: Vec<String>,

    pub group: Option<String>,
}

impl From<MetricConfig> for ManifestMetricConfig {
    fn from(config: MetricConfig) -> Self {
        Self {
            enabled: config.enabled.unwrap_or(true),
            meta: config.meta,
            tags: match config.tags {
                Some(StringOrArrayOfStrings::ArrayOfStrings(ref tags)) => tags.clone(),
                Some(StringOrArrayOfStrings::String(ref tag)) => vec![tag.clone()],
                None => vec![],
            },
            group: config.group,
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ManifestSemanticModelNodeBaseAttributes {
    // Derived
    pub depends_on: NodeDependsOn,

    #[serde(default)]
    pub refs: Vec<DbtRef>,

    #[serde(default)]
    pub unrendered_config: BTreeMap<String, YmlValue>,

    #[serde(default)]
    pub created_at: f64,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ManifestSemanticModelConfig {
    pub enabled: bool,

    pub meta: Option<BTreeMap<String, YmlValue>>,

    pub group: Option<String>,
}

impl From<SemanticModelConfig> for ManifestSemanticModelConfig {
    fn from(config: SemanticModelConfig) -> Self {
        Self {
            enabled: config.enabled.unwrap_or(true),
            meta: config.meta,
            group: config.group,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ManifestSemanticModel {
    pub __common_attr__: ManifestCommonAttributes,
    pub __base_attr__: ManifestSemanticModelNodeBaseAttributes,

    // Semantic Model Specific Attributes
    pub model: String,
    pub node_relation: Option<NodeRelation>,
    pub label: Option<String>,
    pub defaults: Option<SemanticModelDefaults>,
    pub entities: Vec<SemanticEntity>,
    pub measures: Vec<ManifestSemanticModelMeasure>,
    pub dimensions: Vec<crate::schemas::common::Dimension>,
    pub metadata: Option<SourceFileMetadata>,
    pub primary_entity: Option<String>,
    pub group: Option<String>,

    pub config: ManifestSemanticModelConfig,

    pub __other__: BTreeMap<String, YmlValue>,
}

impl From<DbtSemanticModel> for ManifestSemanticModel {
    fn from(semantic_model: DbtSemanticModel) -> Self {
        Self {
            __common_attr__: ManifestCommonAttributes {
                unique_id: semantic_model.__common_attr__.unique_id,
                name: semantic_model.__common_attr__.name,
                package_name: semantic_model.__common_attr__.package_name,
                fqn: semantic_model.__common_attr__.fqn,
                path: semantic_model.__common_attr__.path,
                original_file_path: semantic_model.__common_attr__.original_file_path,
                description: semantic_model.__common_attr__.description,
                tags: semantic_model.__common_attr__.tags,
                meta: semantic_model.__common_attr__.meta,
            },
            __base_attr__: ManifestSemanticModelNodeBaseAttributes {
                depends_on: semantic_model.__base_attr__.depends_on,
                refs: semantic_model.__base_attr__.refs,
                unrendered_config: semantic_model.__semantic_model_attr__.unrendered_config,
                created_at: semantic_model.__semantic_model_attr__.created_at,
            },
            label: semantic_model.__semantic_model_attr__.label,
            metadata: semantic_model.__semantic_model_attr__.metadata,
            group: semantic_model.__semantic_model_attr__.group,
            config: semantic_model.deprecated_config.into(),
            __other__: semantic_model.__other__,
            model: semantic_model.__semantic_model_attr__.model,
            node_relation: semantic_model.__semantic_model_attr__.node_relation,
            defaults: semantic_model.__semantic_model_attr__.defaults,
            entities: semantic_model.__semantic_model_attr__.entities,
            measures: semantic_model
                .__semantic_model_attr__
                .measures
                .into_iter()
                .map(ManifestSemanticModelMeasure::from)
                .collect(),
            dimensions: semantic_model.__semantic_model_attr__.dimensions,
            primary_entity: semantic_model.__semantic_model_attr__.primary_entity,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestSemanticModelMeasure {
    pub name: String,
    pub agg: AggregationType,
    pub description: Option<String>,
    pub label: Option<String>,
    pub create_metric: Option<bool>,
    pub expr: Option<String>,
    pub agg_params: Option<MeasureAggregationParameters>,
    pub non_additive_dimension: Option<NonAdditiveDimension>,
    pub agg_time_dimension: Option<String>,
    pub config: Option<SemanticLayerElementConfig>,
}

impl From<SemanticMeasure> for ManifestSemanticModelMeasure {
    fn from(measure: SemanticMeasure) -> Self {
        Self {
            name: measure.name,
            agg: measure.agg,
            description: measure.description,
            label: measure.label,
            create_metric: measure.create_metric,
            expr: measure.expr,
            agg_params: measure.agg_params,
            non_additive_dimension: measure.non_additive_dimension,
            agg_time_dimension: measure.agg_time_dimension,
            config: measure.config,
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ManifestSavedQueryNodeBaseAttributes {
    // Derived
    pub depends_on: NodeDependsOn,

    #[serde(default)]
    pub refs: Vec<DbtRef>,

    #[serde(default)]
    pub unrendered_config: BTreeMap<String, YmlValue>,

    #[serde(default)]
    pub created_at: f64,
}

// #[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ManifestSavedQuery {
    pub __common_attr__: ManifestCommonAttributes,
    pub __base_attr__: ManifestSavedQueryNodeBaseAttributes,

    // Saved Query Specific Attributes
    pub query_params: crate::schemas::manifest::saved_query::SavedQueryParams,
    pub exports: Vec<crate::schemas::manifest::saved_query::SavedQueryExport>,
    pub label: Option<String>,
    pub metadata: Option<SourceFileMetadata>,
    pub group: Option<String>,

    #[serde(default)]
    pub config: SavedQueryConfig,

    pub __other__: BTreeMap<String, YmlValue>,
}

impl From<DbtSavedQuery> for ManifestSavedQuery {
    fn from(saved_query: DbtSavedQuery) -> Self {
        Self {
            __common_attr__: ManifestCommonAttributes {
                unique_id: saved_query.__common_attr__.unique_id,
                name: saved_query.__common_attr__.name,
                package_name: saved_query.__common_attr__.package_name,
                fqn: saved_query.__common_attr__.fqn,
                path: saved_query.__common_attr__.path,
                original_file_path: saved_query.__common_attr__.original_file_path,
                description: saved_query.__common_attr__.description,
                tags: saved_query.__common_attr__.tags,
                meta: saved_query.__common_attr__.meta,
            },
            __base_attr__: ManifestSavedQueryNodeBaseAttributes {
                depends_on: saved_query.__base_attr__.depends_on,
                refs: saved_query.__base_attr__.refs,
                unrendered_config: saved_query.__saved_query_attr__.unrendered_config,
                created_at: saved_query.__saved_query_attr__.created_at,
            },
            query_params: saved_query.__saved_query_attr__.query_params,
            exports: saved_query.__saved_query_attr__.exports,
            label: saved_query.__saved_query_attr__.label,
            metadata: saved_query.__saved_query_attr__.metadata,
            group: saved_query.__saved_query_attr__.group,
            config: saved_query.deprecated_config,
            __other__: saved_query.__other__,
        }
    }
}
