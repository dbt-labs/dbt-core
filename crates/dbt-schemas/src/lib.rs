pub mod constants;
pub mod dbt_types;
pub mod dbt_utils;
pub mod filter;
pub mod man;
pub mod materialization_resolver;
pub mod state;
pub mod stats;

pub mod schemas {
    pub mod columns;
    pub mod common;
    pub mod data_tests;
    pub mod dbt_column;
    pub mod macros;
    pub mod packages;
    mod prev_state;
    pub mod profiles;
    pub mod ref_and_source;
    pub mod relations;
    mod run_results;
    pub mod selectors;
    pub mod serde;
    mod sources;
    pub use prev_state::{ModificationType, PreviousState};
    pub use run_results::{
        ContextRunResult, RunResultOutput, RunResultsArgs, RunResultsArtifact, RunResultsMetadata,
        TimingInfo,
    };

    // Add re-exports from relation_configs
    pub use relations::relation_configs::{
        BaseRelationChangeSet, BaseRelationConfig, ComponentConfig, RelationChangeSet,
        RelationConfigFactory,
    };

    pub mod nodes;
    pub use nodes::{
        CommonAttributes, DbtExposure, DbtExposureAttr, DbtModel, DbtModelAttr, DbtSeed,
        DbtSeedAttr, DbtSnapshot, DbtSnapshotAttr, DbtSource, DbtSourceAttr, DbtTest, DbtTestAttr,
        DbtUnitTest, DbtUnitTestAttr, InternalDbtNode, InternalDbtNodeAttributes,
        InternalDbtNodeWrapper, IntrospectionKind, NodeBaseAttributes, Nodes, TestMetadata,
    };

    pub use sources::{FreshnessResultsArtifact, FreshnessResultsMetadata, FreshnessResultsNode};
    pub mod legacy_catalog {
        mod catalog;
        pub use catalog::CatalogNodeStats;
        pub use catalog::CatalogTable;
        pub use catalog::ColumnMetadata;
        pub use catalog::DbtCatalog;
        pub use catalog::TableMetadata;
        pub use catalog::build_catalog;
    }
    pub mod manifest {
        mod bigquery_partition;
        mod group;
        #[allow(clippy::module_inception)]
        mod manifest;
        mod manifest_nodes;
        pub(crate) mod metric;
        mod operation;
        pub mod postgres;
        pub mod saved_query;
        mod selector;
        pub mod semantic_model;

        // Versioned manifest modules
        pub mod v10;
        pub mod v11;
        pub mod v12;

        pub(crate) mod common;
        pub use bigquery_partition::{
            BigqueryClusterConfig, BigqueryPartitionConfig, BigqueryPartitionConfigInner,
            GrantAccessToTarget, PartitionConfig, Range, RangeConfig, TimeConfig,
        };
        pub use group::ManifestGroup;
        pub use manifest::{
            BaseMetadata, DbtManifest, DbtNode, ManifestMetadata, build_manifest,
            nodes_from_dbt_manifest,
        };
        pub use manifest_nodes::{
            ManifestDataTest, ManifestExposure, ManifestMetric, ManifestModel, ManifestSavedQuery,
            ManifestSeed, ManifestSemanticModel, ManifestSnapshot, ManifestSource,
            ManifestUnitTest,
        };
        pub use metric::DbtMetric;
        pub use operation::DbtOperation;
        pub use saved_query::{DbtSavedQuery, DbtSavedQueryAttr};
        pub use selector::DbtSelector;
        pub use semantic_model::DbtSemanticModel;
        pub use v10::DbtManifestV10;
        pub use v11::DbtManifestV11;
        pub use v12::DbtManifestV12;
    }
    mod dbt_cloud;
    pub use dbt_cloud::{DbtCloudConfig, DbtCloudContext, DbtCloudProject};

    pub mod semantic_layer {
        pub mod metric;
        pub mod saved_query;
        pub mod semantic_manifest;
        pub mod semantic_model;
    }
    pub mod project {
        mod dbt_project;
        pub(crate) mod configs {
            pub mod common;
            pub mod data_test_config;
            pub mod exposure_config;
            pub mod metric_config;
            pub mod model_config;
            pub mod omissible_utils;
            pub mod omissible_utils_tests;
            pub mod saved_query_config;
            pub mod seed_config;
            pub mod semantic_model_config;
            pub mod snapshot_config;
            pub mod source_config;
            pub mod unit_test_config;
        }

        pub use configs::common::WarehouseSpecificNodeConfig;
        pub use configs::data_test_config::{DataTestConfig, ProjectDataTestConfig};
        pub use configs::exposure_config::{ExposureConfig, ProjectExposureConfig};
        pub use configs::metric_config::{MetricConfig, ProjectMetricConfigs};
        pub use configs::model_config::{ModelConfig, ProjectModelConfig};
        pub use configs::saved_query_config::{
            ExportConfigExportAs, SavedQueryCache, SavedQueryConfig,
        };
        pub use configs::seed_config::{ProjectSeedConfig, SeedConfig};
        pub use configs::semantic_model_config::{ProjectSemanticModelConfig, SemanticModelConfig};
        pub use configs::snapshot_config::{
            ProjectSnapshotConfig, SnapshotConfig, SnapshotMetaColumnNames,
        };
        pub use configs::source_config::{ProjectSourceConfig, SourceConfig};
        pub use configs::unit_test_config::{ProjectUnitTestConfig, UnitTestConfig};
        pub use dbt_project::{
            DbtProject, DbtProjectNameOnly, DbtProjectSimplified, DefaultTo, IterChildren,
            ProjectDbtCloudConfig, QueryComment,
        };
    }

    pub mod properties {
        mod data_test_properties;
        mod exposure_properties;
        pub(crate) mod metrics_properties;
        mod model_properties;
        #[allow(clippy::module_inception)]
        mod properties;
        mod saved_queries_properties;
        mod seed_properties;
        mod snapshot_properties;
        mod source_properties;
        mod unit_test_properties;

        pub use data_test_properties::DataTestProperties;
        pub use exposure_properties::ExposureProperties;
        pub use metrics_properties::MetricsProperties;
        pub use model_properties::ModelConstraint;
        pub use model_properties::ModelFreshness;
        pub use model_properties::ModelProperties;
        pub use properties::GroupConfig;
        pub use properties::GroupProperties;
        pub use properties::{
            DbtPropertiesFile, DbtPropertiesFileValues, GetConfig, MinimalSchemaValue,
            MinimalTableValue,
        };
        pub use saved_queries_properties::SavedQueriesProperties;
        pub use seed_properties::SeedProperties;
        pub use snapshot_properties::SnapshotProperties;
        pub use source_properties::{SourceProperties, Tables};
        pub use unit_test_properties::{UnitTestOverrides, UnitTestProperties};
    }

    // TODO: When dbt-schemas dependency on dbt-common is removed, we should move this to dbt_schemas
    pub use dbt_telemetry as telemetry;
}
