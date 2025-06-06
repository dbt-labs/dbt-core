pub mod constants;
pub mod dbt_types;
pub mod dbt_utils;
pub mod man;
pub mod project_configs;
pub mod state;

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
        RunResult, RunResultsArgs, RunResultsArtifact, RunResultsMetadata, TimingInfo,
    };

    pub use sources::{FreshnessResultsArtifact, FreshnessResultsMetadata, FreshnessResultsNode};
    pub mod manifest {
        mod bigquery_partition;
        mod config;
        mod exposure;
        mod group;
        #[allow(clippy::module_inception)]
        mod manifest;
        mod metric;
        mod nodes;
        mod operation;
        mod saved_query;
        mod selector;
        mod semantic_model;

        pub mod common;
        pub use bigquery_partition::{
            BigQueryModelConfig, BigqueryClusterConfig, BigqueryPartitionConfig,
            BigqueryPartitionConfigInner, BigqueryPartitionConfigLegacy, GrantAccessToTarget,
            Range, RangeConfig, TimeConfig,
        };
        pub use config::DbtConfig;
        pub use exposure::DbtExposure;
        pub use group::DbtGroup;
        pub use manifest::{build_manifest, BaseMetadata, DbtManifest, DbtNode, ManifestMetadata};
        pub use metric::DbtMetric;
        pub use nodes::{
            CommonAttributes, DbtModel, DbtSeed, DbtSnapshot, DbtSource, DbtTest, DbtUnitTest,
            InternalDbtNode, IntrospectionKind, ManifestModelConfig, NodeBaseAttributes, Nodes,
        };
        pub use operation::DbtOperation;
        pub use saved_query::DbtSavedQuery;
        pub use selector::DbtSelector;
        pub use semantic_model::DbtSemanticModel;
    }
    mod dbt_cloud;
    pub use dbt_cloud::{DbtCloudConfig, DbtCloudContext, DbtCloudProject};
    pub mod project {
        mod dbt_project;
        mod configs {
            pub mod data_test_config;
            pub mod metric_config;
            pub mod model_config;
            pub mod saved_queries_config;
            pub mod seed_config;
            pub mod snapshot_config;
            pub mod source_config;
            pub mod unit_test_config;
        }

        pub use configs::data_test_config::ProjectDataTestConfig;
        pub use configs::metric_config::ProjectMetricConfigs;
        pub use configs::model_config::ProjectModelConfig;
        pub use configs::saved_queries_config::SavedQueriesConfig;
        pub use configs::seed_config::ProjectSeedConfig;
        pub use configs::snapshot_config::ProjectSnapshotConfig;
        pub use configs::source_config::ProjectSourceConfig;
        pub use configs::unit_test_config::ProjectUnitTestConfig;
        pub use dbt_project::{
            DbtProject, DbtProjectSimplified, ProjectDbtCloudConfig, QueryComment,
        };
    }

    pub mod properties {
        mod metrics_properties;
        mod model_properties;
        #[allow(clippy::module_inception)]
        mod properties;
        mod saved_queries_properties;
        mod seed_properties;
        mod semantic_models_properties;
        mod snapshot_properties;
        mod source_properties;
        mod test_properties;
        mod try_build_config;
        mod unit_test_properties;

        pub use metrics_properties::MetricsProperties;
        pub use model_properties::ModelFreshness;
        pub use model_properties::ModelProperties;
        pub use properties::{
            DbtPropertiesFile, DbtPropertiesFileValues, MinimalSchemaValue, MinimalTableValue,
        };
        pub use saved_queries_properties::SavedQueriesProperties;
        pub use seed_properties::SeedProperties;
        pub use semantic_models_properties::SemanticModelsProperties;
        pub use snapshot_properties::SnapshotProperties;
        pub use source_properties::{SourceProperties, Tables};
        pub use test_properties::{TestProperties, TestPropertiesConfig};
        pub use try_build_config::TryBuildConfig;
        pub use unit_test_properties::UnitTestProperties;
    }
}
