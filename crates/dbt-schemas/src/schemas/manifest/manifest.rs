use chrono::{DateTime, Utc};
use dbt_common::{Span, io_args::StaticAnalysisKind};
use dbt_serde_yaml::UntaggedEnumDeserialize;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;

use crate::{
    dbt_utils::get_dbt_schema_version,
    schemas::{
        CommonAttributes, DbtModel, DbtModelAttr, DbtSeed, DbtSnapshot, DbtSource, DbtTest,
        DbtUnitTest, DbtUnitTestAttr, IntrospectionKind, NodeBaseAttributes, Nodes,
        common::{DbtChecksum, DbtMaterialization, DbtQuoting, NodeDependsOn},
        manifest::manifest_nodes::{
            ManifestDataTest, ManifestModel, ManifestOperation, ManifestSeed, ManifestSnapshot,
        },
        nodes::{AdapterAttr, DbtSeedAttr, DbtSnapshotAttr, DbtSourceAttr, DbtTestAttr},
    },
    state::ResolverState,
};

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, UntaggedEnumDeserialize)]
#[serde(tag = "resource_type")]
#[serde(rename_all = "snake_case")]
pub enum DbtNode {
    Model(ManifestModel),
    Test(ManifestDataTest),
    Snapshot(ManifestSnapshot),
    Seed(ManifestSeed),
    Operation(ManifestOperation),
    Analysis(ManifestModel),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ManifestMetadata {
    pub __base__: BaseMetadata,
    #[serde(default)]
    pub project_name: String,
    pub project_id: Option<String>,
    pub user_id: Option<String>,
    pub send_anonymous_usage_stats: Option<bool>,
    #[serde(default)]
    pub adapter_type: String,
    pub quoting: Option<DbtQuoting>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BaseMetadata {
    pub dbt_schema_version: String,
    pub dbt_version: String,
    pub generated_at: DateTime<Utc>,
    pub invocation_id: Option<String>,
    pub invocation_started_at: Option<DateTime<Utc>>,
    pub env: BTreeMap<String, String>,
}

impl PartialEq for ManifestMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.__base__.env == other.__base__.env
            && self.project_name == other.project_name
            && self.send_anonymous_usage_stats == other.send_anonymous_usage_stats
            && self.adapter_type == other.adapter_type
        // Note: We intentionally skip comparing the following right now:
        // - generated_at (timestamp)
        // - invocation_id (changes each run)
        // - user_id (may change between environments)
        // - dbt_schema_version (changes between versions)
        // - dbt_version (changes between versions)
        // - project_id (changes between environments)
    }
}

impl Eq for ManifestMetadata {}

// Re-export the current version (V12) as the default
pub use super::v12::DbtManifestV12;

// Type aliases for backwards compatibility
pub type DbtManifest = DbtManifestV12;

pub fn serialize_with_resource_type(mut value: YmlValue, resource_type: &str) -> YmlValue {
    if let YmlValue::Mapping(ref mut map, _) = value {
        map.insert(
            YmlValue::string("resource_type".to_string()),
            YmlValue::string(resource_type.to_string()),
        );
    }
    value
}

pub fn build_manifest(invocation_id: &str, resolver_state: &ResolverState) -> DbtManifest {
    DbtManifest {
        metadata: ManifestMetadata {
            __base__: BaseMetadata {
                dbt_schema_version: get_dbt_schema_version("manifest", 20),
                dbt_version: env!("CARGO_PKG_VERSION").to_string(),
                generated_at: Utc::now(),
                invocation_id: Some(invocation_id.to_string()),
                ..Default::default()
            },
            project_name: resolver_state.root_project_name.clone(),
            adapter_type: resolver_state.dbt_profile.db_config.adapter_type(),
            ..Default::default()
        },
        nodes: resolver_state
            .nodes
            .models
            .iter()
            .map(|(id, node)| (id.clone(), DbtNode::Model((**node).clone().into())))
            .chain(
                resolver_state
                    .nodes
                    .tests
                    .iter()
                    .map(|(id, node)| (id.clone(), DbtNode::Test((**node).clone().into()))),
            )
            .chain(
                resolver_state
                    .nodes
                    .snapshots
                    .iter()
                    .map(|(id, node)| (id.clone(), DbtNode::Snapshot((**node).clone().into()))),
            )
            .chain(
                resolver_state
                    .nodes
                    .seeds
                    .iter()
                    .map(|(id, node)| (id.clone(), DbtNode::Seed((**node).clone().into()))),
            )
            .chain(
                resolver_state
                    .nodes
                    .analyses
                    .iter()
                    .map(|(id, node)| (id.clone(), DbtNode::Analysis((**node).clone().into()))),
            )
            .chain(resolver_state.operations.on_run_start.iter().map(|node| {
                (
                    node.__common_attr__.unique_id.clone(),
                    DbtNode::Operation((*node).clone().into_inner().into()),
                )
            }))
            .chain(resolver_state.operations.on_run_end.iter().map(|node| {
                (
                    node.__common_attr__.unique_id.clone(),
                    DbtNode::Operation((*node).clone().into_inner().into()),
                )
            }))
            .collect(),
        sources: resolver_state
            .nodes
            .sources
            .iter()
            .map(|(id, source)| (id.clone(), (**source).clone().into()))
            .collect(),
        exposures: resolver_state
            .nodes
            .exposures
            .iter()
            .map(|(id, exposure)| (id.clone(), (**exposure).clone().into()))
            .collect(),
        // TODO: map from resolver_state.nodes after they are implemented
        semantic_models: BTreeMap::new(),
        metrics: BTreeMap::new(),
        saved_queries: BTreeMap::new(),
        unit_tests: resolver_state
            .nodes
            .unit_tests
            .iter()
            .map(|(id, unit_test)| (id.clone(), (**unit_test).clone().into()))
            .collect(),
        macros: resolver_state.macros.macros.clone(),
        docs: resolver_state.macros.docs_macros.clone(),
        ..Default::default()
    }
}

pub fn nodes_from_dbt_manifest(manifest: DbtManifest, dbt_quoting: DbtQuoting) -> Nodes {
    let mut nodes = Nodes::default();
    // Do not put disabled nodes into the nodes, because all things in Nodes object should be enabled.
    for (unique_id, node) in manifest.nodes {
        match node {
            DbtNode::Model(model) => {
                nodes.models.insert(
                    unique_id,
                    Arc::new(DbtModel {
                        __common_attr__: CommonAttributes {
                            unique_id: model.__common_attr__.unique_id,
                            name: model.__common_attr__.name,
                            name_span: Span::default(),
                            package_name: model.__common_attr__.package_name,
                            path: model.__common_attr__.path,
                            original_file_path: model.__common_attr__.original_file_path,
                            patch_path: model.__common_attr__.patch_path,
                            fqn: model.__common_attr__.fqn,
                            description: model.__common_attr__.description,
                            raw_code: model.__base_attr__.raw_code,
                            checksum: model.__base_attr__.checksum,
                            language: model.__base_attr__.language,
                            tags: model
                                .config
                                .tags
                                .clone()
                                .map(|tags| tags.into())
                                .unwrap_or_default(),
                            meta: model.config.meta.clone().unwrap_or_default(),
                        },
                        __base_attr__: NodeBaseAttributes {
                            database: model.__common_attr__.database,
                            schema: model.__common_attr__.schema,
                            alias: model.__base_attr__.alias,
                            relation_name: model.__base_attr__.relation_name,
                            materialized: model
                                .config
                                .materialized
                                .clone()
                                .unwrap_or(DbtMaterialization::View),
                            static_analysis: StaticAnalysisKind::On,
                            enabled: model.config.enabled.unwrap_or(true),
                            extended_model: false,
                            quoting: model
                                .config
                                .quoting
                                .map(|mut quoting| {
                                    quoting.default_to(&dbt_quoting);
                                    quoting
                                })
                                .unwrap_or(dbt_quoting)
                                .try_into()
                                .expect("DbtQuoting should be set"),
                            quoting_ignore_case: false,
                            persist_docs: model.config.persist_docs.clone(),
                            columns: model.__base_attr__.columns,
                            depends_on: model.__base_attr__.depends_on,
                            refs: model.__base_attr__.refs,
                            sources: model.__base_attr__.sources,
                            metrics: model.__base_attr__.metrics,
                        },
                        __model_attr__: DbtModelAttr {
                            access: model.config.access.clone().unwrap_or_default(),
                            group: model.config.group.clone(),
                            contract: model.config.contract.clone(),
                            incremental_strategy: model.config.incremental_strategy.clone(),
                            freshness: model.config.freshness.clone(),
                            introspection: IntrospectionKind::None,
                            version: model.version,
                            latest_version: model.latest_version,
                            constraints: model.constraints.unwrap_or_default(),
                            deprecation_date: model.deprecation_date,
                            primary_key: model.primary_key.unwrap_or_default(),
                            time_spine: model.time_spine,
                            event_time: model.config.event_time.clone(),
                        },
                        __adapter_attr__: AdapterAttr::from_config_and_dialect(
                            &model.config.__warehouse_specific_config__,
                            &manifest.metadata.adapter_type,
                        ),
                        deprecated_config: model.config,
                        __other__: model.__other__,
                    }),
                );
            }
            DbtNode::Test(test) => {
                nodes.tests.insert(
                    unique_id,
                    Arc::new(DbtTest {
                        __common_attr__: CommonAttributes {
                            unique_id: test.__common_attr__.unique_id,
                            name: test.__common_attr__.name,
                            package_name: test.__common_attr__.package_name,
                            path: test.__common_attr__.path,
                            name_span: Span::default(),
                            original_file_path: test.__common_attr__.original_file_path,
                            patch_path: test.__common_attr__.patch_path,
                            fqn: test.__common_attr__.fqn,
                            description: test.__common_attr__.description,
                            raw_code: test.__base_attr__.raw_code,
                            checksum: test.__base_attr__.checksum,
                            language: test.__base_attr__.language,
                            tags: test
                                .config
                                .tags
                                .clone()
                                .map(|tags| tags.into())
                                .unwrap_or_default(),
                            meta: test.config.meta.clone().unwrap_or_default(),
                        },
                        __base_attr__: NodeBaseAttributes {
                            database: test.__common_attr__.database,
                            schema: test.__common_attr__.schema,
                            alias: test.__base_attr__.alias,
                            relation_name: test.__base_attr__.relation_name,
                            materialized: DbtMaterialization::Test,
                            static_analysis: StaticAnalysisKind::On,
                            enabled: test.config.enabled.unwrap_or(true),
                            extended_model: false,
                            quoting: test
                                .config
                                .quoting
                                .map(|mut quoting| {
                                    quoting.default_to(&dbt_quoting);
                                    quoting
                                })
                                .unwrap_or(dbt_quoting)
                                .try_into()
                                .expect("DbtQuoting should be set"),
                            quoting_ignore_case: false,
                            persist_docs: None,
                            columns: test.__base_attr__.columns,
                            depends_on: test.__base_attr__.depends_on,
                            refs: test.__base_attr__.refs,
                            sources: test.__base_attr__.sources,
                            metrics: test.__base_attr__.metrics,
                        },
                        __test_attr__: DbtTestAttr {
                            column_name: test.column_name,
                            attached_node: test.attached_node,
                            test_metadata: test.test_metadata,
                            file_key_name: test.file_key_name,
                        },
                        deprecated_config: test.config,
                        __other__: test.__other__,
                    }),
                );
            }
            DbtNode::Snapshot(snapshot) => {
                nodes.snapshots.insert(
                    unique_id,
                    Arc::new(DbtSnapshot {
                        __common_attr__: CommonAttributes {
                            unique_id: snapshot.__common_attr__.unique_id,
                            name: snapshot.__common_attr__.name,
                            package_name: snapshot.__common_attr__.package_name,
                            path: snapshot.__common_attr__.path,
                            name_span: Span::default(),
                            original_file_path: snapshot.__common_attr__.original_file_path,
                            patch_path: snapshot.__common_attr__.patch_path,
                            fqn: snapshot.__common_attr__.fqn,
                            description: snapshot.__common_attr__.description,
                            raw_code: snapshot.__base_attr__.raw_code,
                            checksum: snapshot.__base_attr__.checksum,
                            language: snapshot.__base_attr__.language,
                            tags: snapshot
                                .config
                                .tags
                                .clone()
                                .map(|tags| tags.into())
                                .unwrap_or_default(),
                            meta: snapshot.config.meta.clone().unwrap_or_default(),
                        },
                        __base_attr__: NodeBaseAttributes {
                            database: snapshot.__common_attr__.database,
                            schema: snapshot.__common_attr__.schema,
                            alias: snapshot.__base_attr__.alias,
                            relation_name: snapshot.__base_attr__.relation_name,
                            enabled: snapshot.config.enabled.unwrap_or(true),
                            extended_model: false,
                            materialized: snapshot
                                .config
                                .materialized
                                .clone()
                                .unwrap_or(DbtMaterialization::Table),
                            static_analysis: StaticAnalysisKind::On,
                            quoting: snapshot
                                .config
                                .quoting
                                .map(|mut quoting| {
                                    quoting.default_to(&dbt_quoting);
                                    quoting
                                })
                                .unwrap_or(dbt_quoting)
                                .try_into()
                                .expect("DbtQuoting should be set"),
                            quoting_ignore_case: false,
                            persist_docs: snapshot.config.persist_docs.clone(),
                            columns: snapshot.__base_attr__.columns,
                            depends_on: snapshot.__base_attr__.depends_on,
                            refs: snapshot.__base_attr__.refs,
                            sources: snapshot.__base_attr__.sources,
                            metrics: snapshot.__base_attr__.metrics,
                        },
                        __snapshot_attr__: DbtSnapshotAttr {
                            snapshot_meta_column_names: snapshot
                                .config
                                .snapshot_meta_column_names
                                .clone()
                                .unwrap_or_default(),
                        },
                        deprecated_config: snapshot.config,
                        compiled: snapshot.__base_attr__.compiled,
                        compiled_code: snapshot.__base_attr__.compiled_code,
                        __other__: snapshot.__other__,
                    }),
                );
            }
            DbtNode::Seed(seed) => {
                nodes.seeds.insert(
                    unique_id,
                    Arc::new(DbtSeed {
                        __common_attr__: CommonAttributes {
                            unique_id: seed.__common_attr__.unique_id,
                            name: seed.__common_attr__.name,
                            package_name: seed.__common_attr__.package_name,
                            path: seed.__common_attr__.path,
                            name_span: Span::default(),
                            original_file_path: seed.__common_attr__.original_file_path,
                            patch_path: seed.__common_attr__.patch_path,
                            fqn: seed.__common_attr__.fqn,
                            description: seed.__common_attr__.description,
                            raw_code: seed.__base_attr__.raw_code,
                            checksum: seed.__base_attr__.checksum,
                            language: seed.__base_attr__.language,
                            tags: seed
                                .config
                                .tags
                                .clone()
                                .map(|tags| tags.into())
                                .unwrap_or_default(),
                            meta: seed.config.meta.clone().unwrap_or_default(),
                        },
                        __base_attr__: NodeBaseAttributes {
                            database: seed.__common_attr__.database,
                            schema: seed.__common_attr__.schema,
                            alias: seed.__base_attr__.alias,
                            relation_name: seed.__base_attr__.relation_name,
                            materialized: DbtMaterialization::Table,
                            static_analysis: StaticAnalysisKind::On,
                            enabled: seed.config.enabled.unwrap_or(true),
                            quoting: seed
                                .config
                                .quoting
                                .map(|mut quoting| {
                                    quoting.default_to(&dbt_quoting);
                                    quoting
                                })
                                .unwrap_or(dbt_quoting)
                                .try_into()
                                .expect("DbtQuoting should be set"),
                            quoting_ignore_case: false,
                            extended_model: false,
                            persist_docs: seed.config.persist_docs.clone(),
                            columns: seed.__base_attr__.columns,
                            depends_on: seed.__base_attr__.depends_on,
                            refs: seed.__base_attr__.refs,
                            sources: seed.__base_attr__.sources,
                            metrics: seed.__base_attr__.metrics,
                        },
                        __seed_attr__: DbtSeedAttr {
                            quote_columns: seed.config.quote_columns.unwrap_or_default(),
                            column_types: seed.config.column_types.clone(),
                            delimiter: seed.config.delimiter.clone().map(|d| d.into_inner()),
                            root_path: seed.root_path,
                        },
                        deprecated_config: seed.config,
                        __other__: seed.__other__,
                    }),
                );
            }
            DbtNode::Operation(_) => {}
            DbtNode::Analysis(analysis) => {
                nodes.analyses.insert(
                    unique_id,
                    Arc::new(DbtModel {
                        __common_attr__: CommonAttributes {
                            unique_id: analysis.__common_attr__.unique_id,
                            name: analysis.__common_attr__.name,
                            package_name: analysis.__common_attr__.package_name,
                            path: analysis.__common_attr__.path,
                            name_span: Span::default(),
                            original_file_path: analysis.__common_attr__.original_file_path,
                            patch_path: analysis.__common_attr__.patch_path,
                            fqn: analysis.__common_attr__.fqn,
                            description: analysis.__common_attr__.description,
                            raw_code: analysis.__base_attr__.raw_code,
                            checksum: analysis.__base_attr__.checksum,
                            language: analysis.__base_attr__.language,
                            tags: analysis
                                .config
                                .tags
                                .clone()
                                .map(|tags| tags.into())
                                .unwrap_or_default(),
                            meta: analysis.config.meta.clone().unwrap_or_default(),
                        },
                        __base_attr__: NodeBaseAttributes {
                            database: analysis.__common_attr__.database,
                            schema: analysis.__common_attr__.schema,
                            alias: analysis.__base_attr__.alias,
                            relation_name: analysis.__base_attr__.relation_name,
                            materialized: analysis
                                .config
                                .materialized
                                .clone()
                                .unwrap_or(DbtMaterialization::View),
                            static_analysis: StaticAnalysisKind::On,
                            enabled: analysis.config.enabled.unwrap_or(true),
                            extended_model: false,
                            quoting: analysis
                                .config
                                .quoting
                                .map(|mut quoting| {
                                    quoting.default_to(&dbt_quoting);
                                    quoting
                                })
                                .unwrap_or(dbt_quoting)
                                .try_into()
                                .expect("DbtQuoting should be set"),
                            quoting_ignore_case: false,
                            persist_docs: analysis.config.persist_docs.clone(),
                            columns: analysis.__base_attr__.columns,
                            depends_on: analysis.__base_attr__.depends_on,
                            refs: analysis.__base_attr__.refs,
                            sources: analysis.__base_attr__.sources,
                            metrics: analysis.__base_attr__.metrics,
                        },
                        __model_attr__: DbtModelAttr {
                            access: analysis.config.access.clone().unwrap_or_default(),
                            group: analysis.config.group.clone(),
                            contract: analysis.config.contract.clone(),
                            incremental_strategy: analysis.config.incremental_strategy.clone(),
                            freshness: analysis.config.freshness.clone(),
                            introspection: IntrospectionKind::None,
                            version: analysis.version,
                            latest_version: analysis.latest_version,
                            constraints: analysis.constraints.unwrap_or_default(),
                            deprecation_date: analysis.deprecation_date,
                            primary_key: analysis.primary_key.unwrap_or_default(),
                            time_spine: analysis.time_spine,
                            event_time: analysis.config.event_time.clone(),
                        },
                        __adapter_attr__: AdapterAttr::from_config_and_dialect(
                            &analysis.config.__warehouse_specific_config__,
                            &manifest.metadata.adapter_type,
                        ),
                        deprecated_config: analysis.config,
                        __other__: analysis.__other__,
                    }),
                );
            }
        }
    }
    for (unique_id, source) in manifest.sources {
        nodes.sources.insert(
            unique_id,
            Arc::new(DbtSource {
                __common_attr__: CommonAttributes {
                    unique_id: source.__common_attr__.unique_id,
                    name: source.__common_attr__.name,
                    package_name: source.__common_attr__.package_name,
                    path: source.__common_attr__.path,
                    name_span: Span::default(),
                    original_file_path: source.__common_attr__.original_file_path,
                    patch_path: source.__common_attr__.patch_path,
                    fqn: source.__common_attr__.fqn,
                    description: source.__common_attr__.description,
                    raw_code: None,
                    checksum: DbtChecksum::default(),
                    language: None,
                    tags: source
                        .config
                        .tags
                        .clone()
                        .map(|tags| tags.into())
                        .unwrap_or_default(),
                    meta: source.config.meta.clone().unwrap_or_default(),
                },
                __base_attr__: NodeBaseAttributes {
                    database: source.__common_attr__.database,
                    schema: source.__common_attr__.schema,
                    alias: source.identifier.clone(),
                    relation_name: source.relation_name,
                    materialized: DbtMaterialization::Table,
                    static_analysis: StaticAnalysisKind::On,
                    enabled: source.config.enabled.unwrap_or(true),
                    extended_model: false,
                    quoting: source
                        .quoting
                        .map(|mut quoting| {
                            quoting.default_to(&dbt_quoting);
                            quoting
                        })
                        .unwrap_or(dbt_quoting)
                        .try_into()
                        .expect("DbtQuoting should be set"),
                    quoting_ignore_case: false,
                    persist_docs: None,
                    columns: source.columns,
                    depends_on: NodeDependsOn::default(),
                    refs: vec![],
                    sources: vec![],
                    metrics: vec![],
                },
                __source_attr__: DbtSourceAttr {
                    identifier: source.identifier,
                    source_name: source.source_name,
                    source_description: source.source_description,
                    loader: source.loader,
                    loaded_at_field: source.loaded_at_field,
                    loaded_at_query: source.loaded_at_query,
                    freshness: source.freshness,
                },
                deprecated_config: source.config,
                __other__: source.__other__,
            }),
        );
    }
    for (unique_id, exposure) in manifest.exposures {
        nodes.exposures.insert(
            unique_id,
            Arc::new(crate::schemas::nodes::DbtExposure {
                __common_attr__: CommonAttributes {
                    name: exposure.__common_attr__.name,
                    package_name: exposure.__common_attr__.package_name,
                    path: exposure.__common_attr__.path,
                    name_span: Span::default(),
                    original_file_path: exposure.__common_attr__.original_file_path,
                    patch_path: None,
                    unique_id: exposure.__common_attr__.unique_id,
                    fqn: exposure.__common_attr__.fqn,
                    description: exposure.__common_attr__.description,
                    checksum: Default::default(),
                    language: None,
                    raw_code: None,
                    tags: vec![],
                    meta: BTreeMap::new(),
                },
                __base_attr__: NodeBaseAttributes {
                    database: "".to_string(),
                    schema: "".to_string(),
                    alias: "".to_string(),
                    relation_name: None,
                    quoting: Default::default(),
                    materialized: Default::default(),
                    static_analysis: Default::default(),
                    enabled: true,
                    extended_model: false,
                    persist_docs: None,
                    columns: BTreeMap::new(),
                    refs: exposure.__base_attr__.refs,
                    sources: exposure.__base_attr__.sources,
                    metrics: exposure.__base_attr__.metrics,
                    depends_on: exposure.__base_attr__.depends_on,
                    quoting_ignore_case: false,
                },
                __exposure_attr__: crate::schemas::nodes::DbtExposureAttr {
                    owner: exposure.owner,
                    label: exposure.label,
                    maturity: exposure.maturity,
                    type_: exposure.type_,
                    url: exposure.url,
                    unrendered_config: exposure.__base_attr__.unrendered_config,
                    created_at: exposure.__base_attr__.created_at,
                },
                deprecated_config: exposure.config,
            }),
        );
    }
    for (unique_id, unit_test) in manifest.unit_tests {
        nodes.unit_tests.insert(
            unique_id,
            Arc::new(DbtUnitTest {
                __common_attr__: CommonAttributes {
                    unique_id: unit_test.__common_attr__.unique_id,
                    name: unit_test.__common_attr__.name,
                    package_name: unit_test.__common_attr__.package_name,
                    path: unit_test.__common_attr__.path,
                    name_span: Span::default(),
                    original_file_path: unit_test.__common_attr__.original_file_path,
                    patch_path: unit_test.__common_attr__.patch_path,
                    fqn: unit_test.__common_attr__.fqn,
                    description: unit_test.__common_attr__.description,
                    raw_code: unit_test.__base_attr__.raw_code,
                    checksum: unit_test.__base_attr__.checksum,
                    language: unit_test.__base_attr__.language,
                    tags: unit_test
                        .config
                        .tags
                        .clone()
                        .map(|tags| tags.into())
                        .unwrap_or_default(),
                    meta: unit_test.config.meta.clone().unwrap_or_default(),
                },
                __base_attr__: NodeBaseAttributes {
                    database: unit_test.__common_attr__.database,
                    schema: unit_test.__common_attr__.schema,
                    alias: unit_test.__base_attr__.alias,
                    relation_name: unit_test.__base_attr__.relation_name,
                    materialized: DbtMaterialization::Table,
                    static_analysis: StaticAnalysisKind::On,
                    quoting: dbt_quoting.try_into().expect("DbtQuoting should be set"),
                    quoting_ignore_case: false,
                    enabled: unit_test.config.enabled.unwrap_or(true),
                    extended_model: false,
                    persist_docs: None,
                    columns: unit_test.__base_attr__.columns,
                    depends_on: unit_test.__base_attr__.depends_on,
                    refs: unit_test.__base_attr__.refs,
                    sources: unit_test.__base_attr__.sources,
                    metrics: unit_test.__base_attr__.metrics,
                },
                __unit_test_attr__: DbtUnitTestAttr {
                    model: unit_test.model,
                    given: unit_test.given,
                    expect: unit_test.expect,
                    versions: unit_test.versions,
                    version: unit_test.version,
                    overrides: unit_test.overrides,
                },
                deprecated_config: unit_test.config,
            }),
        );
    }
    for (_unique_id, _metric) in manifest.metrics {
        // TODO: insert DbtMetric into node.metrics
    }

    nodes
}
