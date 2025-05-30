use dbt_common::FsResult;

use crate::schemas::manifest::DbtConfig;

use super::{ModelProperties, SnapshotProperties, TestProperties};

pub trait TryBuildConfig {
    fn try_build_config(&self) -> FsResult<Option<DbtConfig>>;
    fn resource_name() -> &'static str;
}

impl TryBuildConfig for ModelProperties {
    fn try_build_config(&self) -> FsResult<Option<DbtConfig>> {
        if let Some(config) = &self.config {
            Ok(Some(DbtConfig::try_from(config)?))
        } else {
            Ok(None)
        }
    }

    fn resource_name() -> &'static str {
        "model"
    }
}

impl TryBuildConfig for SnapshotProperties {
    fn try_build_config(&self) -> FsResult<Option<DbtConfig>> {
        if let Some(config) = &self.config {
            Ok(Some(DbtConfig::try_from(config)?))
        } else {
            Ok(None)
        }
    }

    fn resource_name() -> &'static str {
        "snapshot"
    }
}

impl TryBuildConfig for TestProperties {
    fn try_build_config(&self) -> FsResult<Option<DbtConfig>> {
        if let Some(config) = &self.config {
            Ok(Some(DbtConfig::try_from(config)?))
        } else {
            Ok(None)
        }
    }

    fn resource_name() -> &'static str {
        "test"
    }
}
