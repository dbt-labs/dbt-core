use std::collections::BTreeMap;

use dbt_common::FsError;
use dbt_serde_yaml::Value;

use crate::schemas::{
    manifest::DbtConfig,
    project::{
        ProjectDataTestConfig, ProjectModelConfig, ProjectSeedConfig, ProjectSnapshotConfig,
        ProjectSourceConfig, ProjectUnitTestConfig,
    },
};

/// This enum is used to represent the different types of configs that can be
/// found in a dbt project. It is used to propagate configs from parent to child nodes
/// and to render SQL files.
///
/// Note the use of the lifetime parameter 'a. This is because the configs are
/// stored in the dbt_project.yml file and do not implement `Clone`.
///
/// TODO: Update the generated types to implement clone or create an intermediary
/// type that can be owned here.
#[derive(Debug)]
pub enum ProjectConfigs<'a> {
    ModelConfigs(&'a ProjectModelConfig),
    DataTestConfigs(&'a ProjectDataTestConfig),
    SeedConfigs(&'a ProjectSeedConfig),
    SnapshotConfigs(&'a ProjectSnapshotConfig),
    SourceConfigs(&'a ProjectSourceConfig),
    UnitTestConfigs(&'a ProjectUnitTestConfig),
}

impl ProjectConfigs<'_> {
    pub fn additional_properties(&self) -> &BTreeMap<String, Value> {
        match self {
            ProjectConfigs::ModelConfigs(model_configs) => &model_configs.__additional_properties__,
            ProjectConfigs::DataTestConfigs(data_test_configs) => {
                &data_test_configs.__additional_properties__
            }
            ProjectConfigs::SeedConfigs(seed_configs) => &seed_configs.__additional_properties__,
            ProjectConfigs::SnapshotConfigs(snapshot_configs) => {
                &snapshot_configs.__additional_properties__
            }
            ProjectConfigs::SourceConfigs(source_configs) => {
                &source_configs.__additional_properties__
            }
            ProjectConfigs::UnitTestConfigs(unit_test_configs) => {
                &unit_test_configs.__additional_properties__
            }
        }
    }
}

impl<'a> TryInto<DbtConfig> for &'a ProjectConfigs<'a> {
    type Error = Box<FsError>;

    fn try_into(self) -> Result<DbtConfig, Self::Error> {
        match self {
            ProjectConfigs::DataTestConfigs(data_test) => DbtConfig::try_from(*data_test),
            ProjectConfigs::ModelConfigs(model) => DbtConfig::try_from(*model),
            ProjectConfigs::SeedConfigs(seed) => DbtConfig::try_from(*seed),
            ProjectConfigs::SnapshotConfigs(snapshot) => DbtConfig::try_from(*snapshot),
            ProjectConfigs::SourceConfigs(source) => DbtConfig::try_from(*source),
            ProjectConfigs::UnitTestConfigs(unit_test) => DbtConfig::try_from(*unit_test),
        }
    }
}
